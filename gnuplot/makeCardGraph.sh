#!/usr/local/bin/gnuplot
reset
set terminal png size 1000,400

set xlabel "Actual cardinality"
set logscale x

set ylabel "Relative error (%)"
set yrange [0:2]

set title "Cardinality error"
set grid

set style data lines

plot "test.dat" using 1:2 title "hll++ - threshold=100", \
"" using 1:3 title "hll++ - threshold=1000", \
"" using 1:4 title "hll++ - threshold=10000", \
"" using 1:5 title "hllb - threshold=100", \
"" using 1:6 title "hllb - threshold=1000", \
"" using 1:7 title "hllb - threshold=10000", \
#
