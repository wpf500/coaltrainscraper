#!/bin/bash -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

date
cd $DIR
. env/bin/activate
python main.py $(date -Idate -d yesterday)
