#!/bin/bash

sudo bash build.sh release  --init --make

cd build_release

sudo make install DESTDIR=.

sudo obd mirror create -n oceanbase-ce -V 4.0.0.0 -p ./usr/local/ -f -t final_2022
