#!/usr/local/bin/gnuplot
reset
set terminal png size 1000,400

set xlabel "Actual cardinality"
set logscale x

set ylabel "Relative error (%)"
set yrange [0:1]

set title "Cardinality error"
set grid

set style data lines

plot "hllBBenchmark18.dat" using 1:2 title "hll++", \
"" using 1:3 title "hllB", \
#
