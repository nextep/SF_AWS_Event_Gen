import json
import random
import datetime

# Number of events and days
num_events = 10000
num_days = 15

# Fixed string for hostnames
hostname_prefix = "hostname"
destination_prefix = "Destination"
DS = 'Windows'

# List of IP sources and destinations
ip_sources = ['192.168.0.{}'.format(i) for i in range(1, 15)]
ip_destinations = ['10.0.0.{}'.format(i) for i in range(1, 2)]

# IP source and destination injection
new_ip_sources = ['192.168.0.169']
new_ip_destinations = ['18.9.45.169']

# Hostname map for new IP addresses
new_ip_hostnames = {
    '192.168.0.169': 'EvilWindow',
    '18.9.45.169': 'CobaltStrike.hax0rz.ru.serious'
}

# Combine original and new IP sources and destinations
ip_sources += new_ip_sources
ip_destinations += new_ip_destinations

# IP source to remove (configure as desired)
ip_source_to_remove = '192.168.0.39'

# Remove IP source if specified
if ip_source_to_remove in ip_sources:
    ip_sources.remove(ip_source_to_remove)

# Timestamp range in seconds
start_time = datetime.datetime(2023, 5, 22)
end_time = datetime.datetime(2023, 5, num_days + 1)

# Generate and write events to JSON files
for day in range(num_days):
    current_time = start_time + datetime.timedelta(days=day)
    filename = DS + '-' + current_time.strftime('%Y-%m-%d') + '.json'
    
    events = []
    for _ in range(num_events):
        timestamp = current_time + datetime.timedelta(seconds=random.randint(0, 86400))
        ip_source = random.choice(ip_sources)
        ip_destination = random.choice(ip_destinations)
        
        source_last_bit = int(ip_source.split('.')[-1])
        destination_last_bit = int(ip_destination.split('.')[-1])
        
        if ip_source in new_ip_hostnames:
            source_hostname = new_ip_hostnames[ip_source]
        else:
            source_hostname = f"{hostname_prefix}{source_last_bit}"
            
        if ip_destination in new_ip_hostnames:
            destination_hostname = new_ip_hostnames[ip_destination]
        else:
            destination_hostname = f"{destination_prefix}{destination_last_bit}"
        
        event = {
            'timestamp': timestamp.isoformat(),
            'data_source': DS,
            'source_ip': ip_source,
            'source_hostname': source_hostname,
            'destination_ip': ip_destination,
            'destination_hostname': destination_hostname,
            'event_type': 'Security',
            'event_id': 'Event',
            'event_version': '1.0',
            'event_priority': 100,
            'event_description': 'Event Description'
        }
        events.append(event)

    with open(filename, 'w') as jsonfile:
        json.dump(events, jsonfile, indent=4)
