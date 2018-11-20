package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * This test verifies behavior around the switch from AVLTree to MergingTree TDigest implementations.  During a rolling upgrade, a cluster
 * could run an aggregation which mixes these two implementations.  This class aims to test the behavior of that situation.
 *
 * This test approaches the problem by first generating a data distribution, adding this to a TDigest, serializing the TDigest, and finally
 * deserializing it in the opposite implementation.  It then asks for a random quantile from both the pre- and post-serialized data
 * structures, as well as computing an exact value from the original distribution.  Pre and post serialization errors are then computed
 * and compared, ensuring they are within a fixed bound of each other.
 */
public class TDigestStateTransitionalSerializationTests extends ESTestCase {

    private static final int POPULATION_SIZE = 10000;
    private static final double TOLERANCE = 0.05;

    private List<Double> sampleData;

    private double getCompression() {
        return 100;
    }

    private enum DistributionMode {
        RANDOM {
            public List<Double> generatePopulation(int populationSize) {
                List<Double> sampleData = new ArrayList<>(populationSize);
                for (int i = 0; i < populationSize; i++) {
                    sampleData.add(randomDouble());
                }
                return sampleData;
            }
        },
        GAUSSIAN {
            public List<Double> generatePopulation(int populationSize) {
                List<Double> sampleData = new ArrayList<>(populationSize);
                for (int i = 0; i < populationSize; i++) {
                    sampleData.add(random().nextGaussian());
                }
                return sampleData;
            }
        },
        SEQUENTIAL {
            public List<Double> generatePopulation(int populationSize) {
                List<Double> sampleData = new ArrayList<>(populationSize);
                for (int i = 0; i < populationSize; i++) {
                    sampleData.add((double) i);
                }
                return sampleData;

            }
        },
        REPEATING {
            public List<Double> generatePopulation(int populationSize) {
                List<Double> sampleData = new ArrayList<>(populationSize);
                double nextVal = randomDouble();
                for (int i = 0; i < populationSize; i++) {
                    sampleData.add(nextVal);
                    if (randomInt(10) < 9) {
                        nextVal = randomDouble();
                    }
                }
                return sampleData;
            }
        };

        abstract public List<Double> generatePopulation(int populationSize);

        static public Set<DistributionMode> allModes() {
            return EnumSet.of(RANDOM, GAUSSIAN, SEQUENTIAL, REPEATING);
        }
    }

    @Before
    public void setup() {
        DistributionMode mode = randomFrom(DistributionMode.allModes());
        sampleData = mode.generatePopulation(POPULATION_SIZE);
    }

    @Test
    public void testAVLToMerge() throws IOException {
        // Not totally sure random compression is the right choice here
        LegacyTDigestState legacy = new LegacyTDigestState(getCompression());
        for (Double value : sampleData) {
            legacy.add(value);
        }
        // Note - sort the data after adding it to the t-digest, because insertion order matters for t-digest
        Collections.sort(sampleData);
        BytesStreamOutput ser = new BytesStreamOutput();
        LegacyTDigestState.write(legacy, ser);
        TDigestState modern = TDigestState.read(ser.bytes().streamInput());
        int q = randomIntBetween((int) Math.floor(0.9 * POPULATION_SIZE), POPULATION_SIZE - 1);
        double quantile = (double) q / POPULATION_SIZE;

        double actual_value = sampleData.get(q);
        double legacy_value = legacy.quantile(quantile);
        double modern_value = modern.quantile(quantile);

        double legacy_error = Math.abs(legacy_value - actual_value) / actual_value;
        double modern_error = Math.abs(modern_value - actual_value) / actual_value;

        assertEquals(legacy_error, modern_error, TOLERANCE);
    }

    @Test
    public void testMergeToAVL() throws IOException {
        // Not totally sure random compression is the right choice here
        TDigestState modern = new TDigestState(getCompression());
        for (Double value : sampleData) {
            modern.add(value);
        }
        // Note - sort the data after adding it to the t-digest, because insertion order matters for t-digest
        Collections.sort(sampleData);
        BytesStreamOutput ser = new BytesStreamOutput();
        TDigestState.write(modern, ser);
        LegacyTDigestState legacy = LegacyTDigestState.read(ser.bytes().streamInput());
        int q = randomIntBetween((int) Math.floor(0.9 * POPULATION_SIZE), POPULATION_SIZE - 1);
        double quantile = (double) q / POPULATION_SIZE;

        double actual_value = sampleData.get(q);
        double legacy_value = legacy.quantile(quantile);
        double modern_value = modern.quantile(quantile);

        double legacy_error = Math.abs(legacy_value - actual_value) / actual_value;
        double modern_error = Math.abs(modern_value - actual_value) / actual_value;

        assertEquals(legacy_error, modern_error, TOLERANCE);
    }

}
