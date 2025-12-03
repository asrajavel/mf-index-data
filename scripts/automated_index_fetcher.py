#!/usr/bin/env python3
"""
Automated Nifty Index Data Fetcher
This script automatically handles cookie management and fetches index data
without requiring manual cookie updates from browser.
"""

import json
import requests
import time
from datetime import datetime
import os
from pathlib import Path

class NiftyIndexFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://www.niftyindices.com"
        self.api_url = f"{self.base_url}/Backpage.aspx/getTotalReturnIndexString"
        self.setup_session()
        
    def setup_session(self):
        """Setup session with required headers"""
        self.session.headers.update({
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=UTF-8',
            'Origin': self.base_url,
            'Referer': f'{self.base_url}/reports/historical-data',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        })

    def get_fresh_cookies(self):
        """Get fresh cookies by visiting the main page"""
        try:
            print("Getting fresh cookies...")
            # Visit the main historical data page to get session cookies
            response = self.session.get(f'{self.base_url}/reports/historical-data')
            if response.status_code == 200:
                print("✓ Fresh cookies obtained successfully")
                return True
            else:
                print(f"✗ Failed to get fresh cookies: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ Error getting fresh cookies: {e}")
            return False

    def fetch_index_data(self, index_name, start_date='01-Jan-1995', end_date=None):
        """Fetch data for a specific index"""
        if end_date is None:
            end_date = datetime.now().strftime('%d-%b-%Y')
            
        payload = {
            "cinfo": json.dumps({
                'name': index_name,
                'startDate': start_date,
                'endDate': end_date,
                'indexName': index_name
            })
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.post(self.api_url, json=payload)
                
                if response.status_code == 200:
                    data = response.json()
                    # Check if we got valid data (not empty)
                    if data.get('d') and data['d'] != '[]':
                        return data
                    else:
                        print(f"  Empty data received for {index_name}, attempt {attempt + 1}")
                        
                elif response.status_code == 500:
                    print(f"  Server error for {index_name}, attempt {attempt + 1}")
                    
                else:
                    print(f"  HTTP {response.status_code} for {index_name}, attempt {attempt + 1}")
                    
                # If we get here, something went wrong, try refreshing cookies
                if attempt < max_retries - 1:
                    print("  Refreshing cookies and retrying...")
                    self.get_fresh_cookies()
                    time.sleep(2)
                    
            except requests.exceptions.RequestException as e:
                print(f"  Request error for {index_name}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    
        return None

    def save_index_data(self, index_name, data, output_dir="../index data"):
        """Save index data to JSON file with count comparison"""
        try:
            # Create output directory if it doesn't exist
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Clean filename (replace only problematic characters, keep spaces)
            filename = index_name.replace('/', '-')
            filepath = f"{output_dir}/{filename}.json"
            
            # Count new data items
            new_count = 0
            if data and 'd' in data:
                if isinstance(data['d'], list):
                    new_count = len(data['d'])
                elif isinstance(data['d'], str) and data['d'] != '[]':
                    # Try to parse if it's a JSON string
                    try:
                        parsed_d = json.loads(data['d'])
                        if isinstance(parsed_d, list):
                            new_count = len(parsed_d)
                    except:
                        new_count = 1 if data['d'] else 0
            
            # Check existing file and count
            old_count = 0
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        if existing_data and 'd' in existing_data:
                            if isinstance(existing_data['d'], list):
                                old_count = len(existing_data['d'])
                            elif isinstance(existing_data['d'], str) and existing_data['d'] != '[]':
                                try:
                                    parsed_d = json.loads(existing_data['d'])
                                    if isinstance(parsed_d, list):
                                        old_count = len(parsed_d)
                                except:
                                    old_count = 1 if existing_data['d'] else 0
                except:
                    old_count = 0  # If file exists but can't be read
            
            # Print count comparison with colors
            if old_count == new_count:
                count_status = "\033[94m✓ same\033[0m"  # Blue
            elif new_count > old_count:
                count_status = f"\033[92m↑ +{new_count - old_count}\033[0m"  # Green
            else:
                count_status = f"\033[91m↓ -{old_count - new_count}\033[0m"  # Red
            
            print(f"  📊 Data count: {old_count} → {new_count} ({count_status})")
            
            # Save the new data
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
            print(f"  ✓ Data saved to {filepath}")
            return {
                'success': True,
                'old_count': old_count,
                'new_count': new_count,
                'change': new_count - old_count
            }
            
        except Exception as e:
            print(f"  ✗ Error saving data for {index_name}: {e}")
            return {'success': False}

    def load_index_list(self, filename="../index list.json"):
        """Load the list of indices to fetch"""
        try:
            with open(filename, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                return data.get('d', [])
        except Exception as e:
            print(f"Error loading index list: {e}")
            return []

    def load_index_mapping(self, filename="../index mapping.json"):
        """Load index name mapping"""
        try:
            with open(filename, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                return {item['Index_long_name'].upper(): item['Trading_Index_Name'] 
                       for item in data}
        except Exception as e:
            print(f"Error loading index mapping: {e}")
            return {}



    def display_change_summary(self, change_tracking, failed_indices, interrupted=False):
        """Display summary grouped by exact change amounts"""
        title = "📊 EXECUTION SUMMARY - BY EXACT CHANGES"
        if interrupted:
            title += " (⚠️ INTERRUPTED)"
        print(f"\n{title}")
        print("=" * 50)
        
        # Sort changes by amount (largest positive first, then zero, then negative)
        sorted_changes = sorted(change_tracking.keys(), key=lambda x: (-x if x > 0 else (0 if x == 0 else 1000 + abs(x))))
        
        # Group by categories
        new_files = {}
        increases = {}
        no_changes = {}
        decreases = {}
        
        for change in sorted_changes:
            indices = change_tracking[change]
            if change > 0:
                # Check if these might be new files (we could enhance this logic)
                increases[change] = indices
            elif change == 0:
                no_changes[change] = indices
            else:
                decreases[change] = indices
        
        # Display increases
        if increases:
            print("\n📈 INCREASED DATA:")
            for change in sorted(increases.keys(), reverse=True):
                indices_str = ", ".join(increases[change])
                print(f"\033[92m+{change}\033[0m -> ({indices_str})")
        
        # Display no changes
        if no_changes:
            print("\n🔵 NO CHANGES:")
            for change in no_changes:
                indices_str = ", ".join(no_changes[change])
                print(f"\033[94m{change}\033[0m -> ({indices_str})")
        
        # Display decreases
        if decreases:
            print("\n📉 DECREASED DATA:")
            for change in sorted(decreases.keys(), reverse=True):
                indices_str = ", ".join(decreases[change])
                print(f"\033[91m{change}\033[0m -> ({indices_str})")
        
        # Display failures
        if failed_indices:
            print("\n❌ FAILED:")
            indices_str = ", ".join(failed_indices)
            print(f"\033[91mFailed\033[0m -> ({indices_str})")
        
        # Total summary
        total_successful = sum(len(indices) for indices in change_tracking.values())
        total_failed = len(failed_indices)
        print(f"\n📊 Total: {total_successful} successful, {total_failed} failed")
        
        if interrupted:
            print("⚠️  Note: Execution was interrupted - this is a partial summary")

    def fetch_all_indices(self):
        """Fetch data for all indices in the list"""
        # Get fresh cookies first
        if not self.get_fresh_cookies():
            print("Failed to get initial cookies. Exiting.")
            return
            
        # Load index list and mapping
        index_list = self.load_index_list()
        index_mapping = self.load_index_mapping()
        
        if not index_list:
            print("No indices found in index list. Exiting.")
            return
            
        print(f"Found {len(index_list)} indices to fetch")
        print("-" * 50)
        
        # Track results for summary
        successful = 0
        failed = 0
        failed_indices = []
        change_tracking = {}  # {change_amount: [list_of_index_names]}
        
        try:
            for i, index in enumerate(index_list):
                index_name = index.get('indextype', '')
                if not index_name:
                    continue
                    
                print(f"[{i+1}/{len(index_list)}] Fetching: {index_name}")
                
                # Get trading name from mapping if available
                trading_name = index_mapping.get(index_name.upper(), index_name)
                
                # Fetch the data
                data = self.fetch_index_data(trading_name)
                
                if data:
                    save_result = self.save_index_data(index_name, data)
                    if save_result['success']:
                        successful += 1
                        # Track the change for summary
                        change_amount = save_result['change']
                        if change_amount not in change_tracking:
                            change_tracking[change_amount] = []
                        change_tracking[change_amount].append(index_name)
                    else:
                        failed += 1
                        failed_indices.append(index_name)
                else:
                    print(f"  ✗ Failed to fetch data for {index_name}")
                    failed += 1
                    failed_indices.append(index_name)
                    
                # Small delay between requests to be respectful
                time.sleep(1)
                
        except KeyboardInterrupt:
            print(f"\n\n🛑 Interrupted by user (Ctrl+C)")
            print(f"📊 Processed {successful + failed} out of {len(index_list)} indices before interruption")
            
        print("-" * 50)
        print(f"Basic Summary: {successful} successful, {failed} failed")
        
        # Display detailed summary grouped by changes
        self.display_change_summary(change_tracking, failed_indices, interrupted=(successful + failed < len(index_list)))

def main():
    """Main function"""
    print("🚀 Starting Automated Nifty Index Data Fetcher")
    print("=" * 50)
    
    fetcher = NiftyIndexFetcher()
    fetcher.fetch_all_indices()
    
    print("✅ Completed!")

if __name__ == "__main__":
    main() 