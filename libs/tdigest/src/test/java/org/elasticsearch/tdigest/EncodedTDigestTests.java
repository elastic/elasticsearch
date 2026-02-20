/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notANumber;

public class EncodedTDigestTests extends TDigestTestCase {

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
            assertThat(iterator.endReached(), is(true));
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
        assertThat(encoded.cdf(0.0), is(notANumber()));
        assertThat(encoded.quantile(0.0), is(notANumber()));
        assertThat(encoded.quantile(1.0), is(notANumber()));
        EncodedTDigest.CentroidIterator iterator = encoded.centroidIterator();
        assertThat(iterator.endReached(), is(true));
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
        try (TDigest digest = TDigest.createAvlTreeDigest(arrays(), compression)) {
            int values = randomIntBetween(1, 300);
            for (int i = 0; i < values; i++) {
                double value = random().nextGaussian() * 200 + randomDoubleBetween(-100, 100, true);
                long weight = randomIntBetween(1, 4);
                digest.add(value, weight);
            }
            digest.compress();

            // now create a t-digest using just the centroids to ensure there is hidden state influencing the results
            TDigest result = TDigest.createAvlTreeDigest(arrays(), compression);
            for (Centroid centroid : digest.centroids()) {
                result.add(centroid.mean(), centroid.count());
            }
            return result;
        }
    }

    private void assertTDigestsMatch(ReadableTDigest expected, EncodedTDigest encoded) {
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
            assertThat(encoded.cdf(probe), equalTo(expected.cdf(probe)));
        }

        List<Double> quantileProbes = new ArrayList<>();
        quantileProbes.add(0.0);
        quantileProbes.add(1.0);
        quantileProbes.add(0.5);
        for (int i = 0; i < 100; i++) {
            quantileProbes.add(randomDoubleBetween(0.0, 1.0, true));
        }
        for (double q : quantileProbes) {
            assertThat(encoded.quantile(q), equalTo(expected.quantile(q)));
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
