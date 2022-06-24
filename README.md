DPimport: A command line glob importer for DPdash
=================================================
DPimport is a command line tool for importing files into DPdash using a
simple [`glob`](https://en.wikipedia.org/wiki/Glob_(programming)) expression.

## Table of contents
1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Usage](#usage)
4. [MongoDB](#mongodb)

## Installation
Just use `pip`

```bash
pip install https://github.com/AMP-SCZ/dpimport.git
```

## Configuration
DPimport requires a configuration file in YAML format, passed as a command
line argument with `-c|--config`, for establishing a MongoDB database 
connection. You will find an example configuration file in the `examples` 
directory within this repository.

## Usage
The main command line tool is `import.py`. You can use this tool to import any
DPdash-compatible CSV files or metadata files using the direct path to a file 
or a glob expression (use single quotes to avoid shell expansion)

```bash
import.py -c config.yml '/PHOENIX/GENERAL/STUDY_A/SUB_001/DATA_TYPE/processed/*.csv'
```

You may also now use the `**` recursive glob expression, for example:

```bash
import.py -c config.yml '/PHOENIX/**/*.csv'
```

or

```bash
import.py -c config.yml '/PHOENIX/GENERAL/**/processed/*.csv'
```

and so on.

<details>
<summary>Details about the pattern /**/</summary>
<br>

`directory/*/*.csv` matches only `directory/[subdirectory]/[filename].csv`. With a [recursive glob pattern](https://docs.python.org/3/library/glob.html#glob.glob), `directory/**/*.csv` will additionally match:

* `directory/[filename].csv` (no subdirectory)
* `directory/[subdirectory1]/[subdirectory2]/[filename].csv` (sub-subdirectory)

and so on, for as many levels deep as exist in the directory tree.

</details>



## MongoDB

This tool requires MongoDB to be running and accessible with the credentials you
supply in the `config.yml` file. For tips on MongoDB as it is used in DPdash and DPimport,
see [the DPdash wiki](https://github.com/PREDICT-DPACC/dpdash/wiki/MongoDB-Tips).

