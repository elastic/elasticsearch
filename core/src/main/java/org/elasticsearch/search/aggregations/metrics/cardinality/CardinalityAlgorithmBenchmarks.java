package org.elasticsearch.search.aggregations.metrics.cardinality;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.common.util.BigArrays;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class CardinalityAlgorithmBenchmarks {

    private static double error(HyperLogLogPlusPlus h, long expected) {
        double actual = h.cardinality(0);
        return Math.abs(expected - actual) / expected;
    }

    private static double error(HyperLogLogBeta h, long expected) {
        double actual = h.cardinality(0);
        return Math.abs(expected - actual) / expected;
    }

    public static void main(String[] args) throws Exception {
        for (int precision = HyperLogLogBeta.MIN_PRECISION; precision <= HyperLogLogBeta.MAX_PRECISION; precision++) {
            File outFile = new File("/Users/colings86/dev/work/git/elasticsearch/gnuplot/hllBBenchmark" + precision + ".dat");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
                HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

                HyperLogLogBeta hllb = new HyperLogLogBeta(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

                int next = 100;
                int step = 10;

                for (int i = 1; i <= 10000000; ++i) {
                    long h = BitMixer.mix64(i);
                    hllpp.collect(0, h);

                    hllb.collect(0, h);

                    if (i == next) {
                        System.out.println(i + " " + error(hllpp, i) * 100 + " " + error(hllb, i) * 100);
                        writer.write(i + " " + error(hllpp, i) * 100 + " " + error(hllb, i) * 100 + '\n');
                        next += step;
                        if (next >= 100 * step) {
                            step *= 10;
                        }
                    }
                }
            }
        }
    }
}
