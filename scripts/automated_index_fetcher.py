#!/usr/bin/env python3
"""
Automated Nifty Index Data Fetcher
Uses curl_cffi for Chrome TLS fingerprint impersonation to bypass Akamai CDN.
"""

import json
import time
import concurrent.futures
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from curl_cffi import requests as cffi_requests

REPO_ROOT = Path(__file__).parent.parent
BASE_URL = "https://www.niftyindices.com"
API_URL = f"{BASE_URL}/Backpage.aspx/getTotalReturnIndexString"

GET_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml",
}

POST_HEADERS = {
    **GET_HEADERS,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/reports/historical-data",
}


def _count_records(data):
    """Count records in the API response's 'd' field."""
    d = data.get('d') if data else None
    if not d or d == '[]':
        return 0
    if isinstance(d, list):
        return len(d)
    try:
        parsed = json.loads(d)
        return len(parsed) if isinstance(parsed, list) else 1
    except (json.JSONDecodeError, TypeError):
        return 1


class NiftyIndexFetcher:
    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome")
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def _refresh_session(self):
        print("Getting fresh cookies...")
        self.session = cffi_requests.Session(impersonate="chrome")
        try:
            resp = self.session.get(f"{BASE_URL}/reports/historical-data", headers=GET_HEADERS, timeout=30)
            if resp.status_code == 200:
                print("✓ Fresh cookies obtained successfully")
                return True
            print(f"✗ Failed to get fresh cookies: HTTP {resp.status_code}")
        except Exception as e:
            print(f"✗ Error getting fresh cookies: {e}")
        return False

    def fetch_index_data(self, index_name, start_date='01-Jan-1995', end_date=None):
        if end_date is None:
            end_date = datetime.now().strftime('%d-%b-%Y')

        payload = {"cinfo": json.dumps({
            'name': index_name, 'startDate': start_date,
            'endDate': end_date, 'indexName': index_name,
        })}

        for attempt in range(3):
            try:
                future = self.executor.submit(
                    self.session.post, API_URL, headers=POST_HEADERS, json=payload, timeout=45
                )
                try:
                    resp = future.result(timeout=50)
                except concurrent.futures.TimeoutError:
                    print(f"  Hard timeout for {index_name}, attempt {attempt + 1}")
                    if attempt < 2:
                        self._refresh_session()
                        time.sleep(2)
                    continue

                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('d') and data['d'] != '[]':
                        return data
                    print(f"  Empty data for {index_name}, attempt {attempt + 1}")
                else:
                    print(f"  HTTP {resp.status_code} for {index_name}, attempt {attempt + 1}")

            except Exception as e:
                print(f"  Request error for {index_name}: {e}")

            if attempt < 2:
                print("  Refreshing cookies and retrying...")
                self._refresh_session()
                time.sleep(2)

        return None

    def save_index_data(self, index_name, data):
        output_dir = REPO_ROOT / "index data"
        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / f"{index_name.replace('/', '-')}.json"

        # Strip per-row RequestNumber (changes every API call, not meaningful data)
        d = data.get('d')
        if d and d != '[]' and isinstance(d, str):
            try:
                records = json.loads(d)
                if isinstance(records, list):
                    for r in records:
                        r.pop('RequestNumber', None)
                    data = {**data, 'd': json.dumps(records)}
            except (json.JSONDecodeError, TypeError):
                pass

        new_count = _count_records(data)
        old_count = _count_records(json.loads(filepath.read_text(encoding='utf-8'))) if filepath.exists() else 0

        diff = new_count - old_count
        if diff == 0:
            status = "\033[94m✓ same\033[0m"
        elif diff > 0:
            status = f"\033[92m↑ +{diff}\033[0m"
        else:
            status = f"\033[91m↓ {diff}\033[0m"

        print(f"  📊 Data count: {old_count} → {new_count} ({status})")
        filepath.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f"  ✓ Data saved to {filepath}")
        return diff

    def load_index_list(self):
        data = json.loads((REPO_ROOT / "index list.json").read_text(encoding='utf-8-sig'))
        return data.get('d', [])

    def load_index_mapping(self):
        data = json.loads((REPO_ROOT / "index mapping.json").read_text(encoding='utf-8-sig'))
        return {item['Index_long_name'].upper(): item['Trading_Index_Name'] for item in data}

    def display_summary(self, change_tracking, failed_indices, interrupted=False):
        title = "📊 EXECUTION SUMMARY"
        if interrupted:
            title += " (⚠️ INTERRUPTED)"
        print(f"\n{title}")
        print("=" * 50)

        for label, color, predicate in [
            ("📈 INCREASED", "\033[92m", lambda c: c > 0),
            ("🔵 NO CHANGES", "\033[94m", lambda c: c == 0),
            ("📉 DECREASED", "\033[91m", lambda c: c < 0),
        ]:
            group = {c: v for c, v in change_tracking.items() if predicate(c)}
            if group:
                print(f"\n{label}:")
                for change in sorted(group, reverse=True):
                    sign = "+" if change > 0 else ""
                    print(f"  {color}{sign}{change}\033[0m → {', '.join(group[change])}")

        if failed_indices:
            print(f"\n❌ FAILED: {', '.join(failed_indices)}")

        total = sum(len(v) for v in change_tracking.values())
        print(f"\n📊 Total: {total} successful, {len(failed_indices)} failed")
        if interrupted:
            print("⚠️  Execution was interrupted — partial summary")

    def fetch_all_indices(self):
        if not self._refresh_session():
            print("Failed to get initial cookies. Exiting.")
            return

        index_list = self.load_index_list()
        index_mapping = self.load_index_mapping()
        print(f"Found {len(index_list)} indices to fetch")
        print("-" * 50)

        failed_indices = []
        change_tracking = defaultdict(list)
        processed = 0

        try:
            for i, index in enumerate(index_list):
                index_name = index.get('indextype', '')
                if not index_name:
                    continue

                print(f"[{i+1}/{len(index_list)}] Fetching: {index_name}")
                trading_name = index_mapping.get(index_name.upper(), index_name)
                data = self.fetch_index_data(trading_name)

                if data:
                    try:
                        diff = self.save_index_data(index_name, data)
                        change_tracking[diff].append(index_name)
                    except Exception as e:
                        print(f"  ✗ Error saving {index_name}: {e}")
                        failed_indices.append(index_name)
                else:
                    print(f"  ✗ Failed to fetch data for {index_name}")
                    failed_indices.append(index_name)

                processed += 1
                time.sleep(1)

        except KeyboardInterrupt:
            print(f"\n🛑 Interrupted after {processed}/{len(index_list)} indices")

        finally:
            self.executor.shutdown(wait=False)

        successful = sum(len(v) for v in change_tracking.values())
        print("-" * 50)
        print(f"Basic Summary: {successful} successful, {len(failed_indices)} failed")
        self.display_summary(change_tracking, failed_indices, interrupted=(processed < len(index_list)))


def main():
    print("🚀 Starting Automated Nifty Index Data Fetcher")
    print("=" * 50)
    NiftyIndexFetcher().fetch_all_indices()
    print("✅ Completed!")


if __name__ == "__main__":
    main()
