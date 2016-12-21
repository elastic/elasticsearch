package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.util.BigArrays;

import com.carrotsearch.hppc.BitMixer;

public class CardinalityAlgorithmBenchmarks {

    private static double error(HyperLogLogPlusPlus h, long expected) {
        double actual = h.cardinality(0);
        return Math.abs(expected - actual) / expected;
    }

    private static double error(HyperLogLogBeta h, long expected) {
        double actual = h.cardinality(0);
        return Math.abs(expected - actual) / expected;
    }

    public static void main(String[] args) {
        int p100 = HyperLogLogPlusPlus.precisionFromThreshold(100);
        int p1000 = HyperLogLogPlusPlus.precisionFromThreshold(1000);
        int p10000 = HyperLogLogPlusPlus.precisionFromThreshold(10000);
        HyperLogLogPlusPlus hllpp100 = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus hllpp1000 = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus hllpp10000 = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        
        HyperLogLogBeta hllb100 = new HyperLogLogBeta(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogBeta hllb1000 = new HyperLogLogBeta(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogBeta hllb10000 = new HyperLogLogBeta(14, BigArrays.NON_RECYCLING_INSTANCE, 1);

        int next = 100;
        int step = 10;

        for (int i = 1; i <= 10000000; ++i) {
            long h = BitMixer.mix64(i);
            hllpp100.collect(0, h);
            hllpp1000.collect(0, h);
            hllpp10000.collect(0, h);
            
            hllb100.collect(0, h);
            hllb1000.collect(0, h);
            hllb10000.collect(0, h);

            if (i == next) {
                System.out.println(i + " " + error(hllpp100, i) * 100 + " " + error(hllpp1000, i) * 100 + " " + error(hllpp10000, i) * 100
                        + " " + error(hllb100, i) * 100 + " " + error(hllb1000, i) * 100 + " " + error(hllb10000, i) * 100);
                next += step;
                if (next >= 100 * step) {
                    step *= 10;
                }
            }
        }
    }
}
