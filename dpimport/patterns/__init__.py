import re

DATAFILE = re.compile(r'(?P<study>\w+)\-(?P<subject>\w+)\-(?P<assessment>\w+)\-(?P<units>day)(?P<start>[+-]?\d+(?:\.\d+)?)to(?P<end>[+-]?\d+(?:\.\d+)?)(?P<extension>.csv)')

METADATA = re.compile(r'(?P<study>\w+)\_metadata(?P<extension>.csv)')

GLOB_SUB = re.compile(r'(\w+\-\w+\-\w+\-day)[+-]?\d+(?:\.\d+)?to[+-]?\d+(?:\.\d+)?(.*)')
