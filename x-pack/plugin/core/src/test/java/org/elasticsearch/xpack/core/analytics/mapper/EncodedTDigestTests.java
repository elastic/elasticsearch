/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.TDigest;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notANumber;

public class EncodedTDigestTests extends ESTestCase {

    private double quantile(EncodedTDigest tdigest, double quantile) {
        try (TDigest temporary = TDigest.createMergingDigest(arrays(), 1000.0)) {
            temporary.add(tdigest);
            return temporary.quantile(quantile);
        }
    }

    private double cdf(EncodedTDigest tdigest, double x) {
        try (TDigest temporary = TDigest.createMergingDigest(arrays(), 1000.0)) {
            temporary.add(tdigest);
            return temporary.cdf(x);
        }
    }

    public void testEncodingPreservesTDigestProperties() {
        try (TDigest digest = randomDigest()) {
            List<Centroid> centroids = new ArrayList<>(digest.centroids());
            EncodedTDigest encoded = new EncodedTDigest(EncodedTDigest.encodeCentroids(centroids));
            assertTDigestsMatch(digest, encoded);
        }
    }

    public void testResetReplacesEncodedBytes() {
        try (TDigest first = randomDigest(); TDigest second = randomDigest()) {
            List<Centroid> firstCentroids = new ArrayList<>(first.centroids());
            EncodedTDigest encoded = new EncodedTDigest(EncodedTDigest.encodeCentroids(firstCentroids));
            assertTDigestsMatch(first, encoded);

            List<Centroid> secondCentroids = new ArrayList<>(second.centroids());
            encoded.reset(EncodedTDigest.encodeCentroids(secondCentroids));
            assertTDigestsMatch(second, encoded);
        }
    }

    public void testCentroidIteratorMatchesCentroidsMethod() {
        try (TDigest digest = randomDigest()) {
            EncodedTDigest encoded = new EncodedTDigest(EncodedTDigest.encodeCentroids(new ArrayList<>(digest.centroids())));

            List<Centroid> asCollection = new ArrayList<>(encoded.centroids());
            List<Centroid> asIterator = new ArrayList<>();
            EncodedTDigest.CentroidIterator iterator = encoded.centroidIterator();
            while (iterator.next()) {
                asIterator.add(new Centroid(iterator.currentMean(), iterator.currentCount()));
            }
            assertThat(iterator.hasNext(), is(false));
            assertCentroidsEqual(asCollection, asIterator);
        }
    }

    public void testEmptyTDigest() {
        EncodedTDigest encoded = new EncodedTDigest(EncodedTDigest.encodeCentroids(List.of()));
        assertThat(encoded.centroids().isEmpty(), is(true));
        assertThat(encoded.centroidCount(), equalTo(0));
        assertThat(encoded.size(), equalTo(0L));
        assertThat(encoded.encodedDigest().length, equalTo(0));
        assertThat(encoded.getMin(), is(notANumber()));
        assertThat(encoded.getMax(), is(notANumber()));
        assertThat(cdf(encoded, 0.0), is(notANumber()));
        assertThat(quantile(encoded, 0.0), is(notANumber()));
        assertThat(quantile(encoded, 1.0), is(notANumber()));
        EncodedTDigest.CentroidIterator iterator = encoded.centroidIterator();
        assertThat(iterator.hasNext(), is(false));
        assertThat(iterator.next(), is(false));
    }

    public void testEncodeUsingSeparateArrays() {
        try (TDigest digest = randomDigest()) {
            List<Double> values = digest.centroids().stream().map(Centroid::mean).toList();
            List<Long> counts = digest.centroids().stream().map(Centroid::count).toList();
            EncodedTDigest encoded = new EncodedTDigest(EncodedTDigest.encodeCentroids(values, counts));
            assertTDigestsMatch(digest, encoded);
        }
    }

    private TDigest randomDigest() {
        double compression = randomDoubleBetween(20.0, 500.0, true);
        try (TDigest digest = TDigest.createMergingDigest(arrays(), compression)) {
            int values = randomIntBetween(1, 3000);
            for (int i = 0; i < values; i++) {
                double value = random().nextGaussian() * 200 + randomDoubleBetween(-100, 100, true);
                long weight = randomIntBetween(1, 4);
                digest.add(value, weight);
            }
            digest.compress();

            // Create a digest from centroids only to ensure no hidden state affects results.
            TDigest result = TDigest.createMergingDigest(arrays(), compression);
            for (Centroid centroid : digest.centroids()) {
                result.add(centroid.mean(), centroid.count());
            }
            return result;
        }
    }

    private MemoryTrackingTDigestArrays arrays() {
        return new MemoryTrackingTDigestArrays(newLimitedBreaker(ByteSizeValue.ofMb(100)));
    }

    private void assertTDigestsMatch(TDigest expected, EncodedTDigest encoded) {
        assertCentroidsEqual(expected.centroids(), encoded.centroids());
        assertThat(encoded.size(), equalTo(expected.size()));
        assertThat(encoded.getMin(), equalTo(expected.getMin()));
        assertThat(encoded.getMax(), equalTo(expected.getMax()));
        assertThat(encoded.centroidCount(), equalTo(expected.centroidCount()));

        List<Centroid> expectedCentroids = new ArrayList<>(expected.centroids());

        List<Double> cdfProbes = new ArrayList<>();
        for (Centroid centroid : expectedCentroids) {
            cdfProbes.add(centroid.mean());
        }
        if (expectedCentroids.isEmpty() == false) {
            for (int i = 0; i < 40; i++) {
                cdfProbes.add(randomDoubleBetween(expected.getMin() - 1, expected.getMax() + 1, true));
            }
        }
        for (double probe : cdfProbes) {
            assertThat(cdf(encoded, probe), equalTo(expected.cdf(probe)));
        }

        List<Double> quantileProbes = new ArrayList<>();
        quantileProbes.add(0.0);
        quantileProbes.add(1.0);
        quantileProbes.add(0.5);
        for (int i = 0; i < 100; i++) {
            quantileProbes.add(randomDoubleBetween(0.0, 1.0, true));
        }
        for (double q : quantileProbes) {
            assertThat(quantile(encoded, q), equalTo(expected.quantile(q)));
        }
    }

    private static void assertCentroidsEqual(Collection<Centroid> expected, Collection<Centroid> actual) {
        assertThat(actual.size(), equalTo(expected.size()));
        Iterator<Centroid> expectedIterator = expected.iterator();
        Iterator<Centroid> actualIterator = actual.iterator();
        while (expectedIterator.hasNext()) {
            assertThat(actualIterator.hasNext(), is(true));
            Centroid expectedCentroid = expectedIterator.next();
            Centroid actualCentroid = actualIterator.next();
            assertThat(actualCentroid.mean(), equalTo(expectedCentroid.mean()));
            assertThat(actualCentroid.count(), equalTo(expectedCentroid.count()));
        }
        assertThat(actualIterator.hasNext(), is(false));
    }
}
