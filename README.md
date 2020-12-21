DPimport: A command line glob importer for DPdash
=================================================
DPimport is a command line tool for importing files into DPdash using a
simple [`glob`](https://en.wikipedia.org/wiki/Glob_(programming)) expression.

## Table of contents
1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Usage](#usage)

## Installation
Just use `pip`

```bash
pip install git+https://github.com/harvard-nrg/dpimport
```

## Configuration
DPimport requires a configuration file `-c|--config` for establishing a database 
connection. You will find an example configuration file in the `examples` 
directory within this repository.

## Usage
The main command line tool is `import.py`. You can use this tool to import any
DPdash-compatible CSV files or metadata files using the direct path to a file 
or a glob expression (use single quotes to avoid shell expansion)

```bash
import.py -c config.yml '/PHOENIX/GENERAL/STUDY_A/SUB_001/DATA_TYPE/processed/*.csv'
```

