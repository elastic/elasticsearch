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
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notANumber;

public class AbstractCentroidBackedTDigestTests extends TDigestTestCase {

    public void testCdfMatchesWrappedDigest() {
        try (TDigest digest = randomDigest()) {
            ReadableTDigest wrapped = new CentroidListBackedTDigest(new ArrayList<>(digest.centroids()));
            List<Centroid> expectedCentroids = new ArrayList<>(digest.centroids());
            List<Double> cdfProbes = new ArrayList<>();
            for (Centroid centroid : expectedCentroids) {
                cdfProbes.add(centroid.mean());
            }
            if (expectedCentroids.isEmpty() == false) {
                for (int i = 0; i < 40; i++) {
                    cdfProbes.add(randomDoubleBetween(digest.getMin() - 1, digest.getMax() + 1, true));
                }
            }
            for (double probe : cdfProbes) {
                assertThat(wrapped.cdf(probe), equalTo(digest.cdf(probe)));
            }
        }
    }

    public void testQuantileMatchesWrappedDigest() {
        try (TDigest digest = randomDigest()) {
            ReadableTDigest wrapped = new CentroidListBackedTDigest(new ArrayList<>(digest.centroids()));
            List<Double> quantileProbes = new ArrayList<>();
            quantileProbes.add(0.0);
            quantileProbes.add(1.0);
            quantileProbes.add(0.5);
            for (int i = 0; i < 100; i++) {
                quantileProbes.add(randomDoubleBetween(0.0, 1.0, true));
            }
            for (double q : quantileProbes) {
                assertThat(wrapped.quantile(q), equalTo(digest.quantile(q)));
            }
        }
    }

    public void testEmptyDigest() {
        ReadableTDigest wrapped = new CentroidListBackedTDigest(List.of());
        assertThat(wrapped.cdf(0.0), is(notANumber()));
        assertThat(wrapped.quantile(0.0), is(notANumber()));
        assertThat(wrapped.quantile(1.0), is(notANumber()));
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

            // Build from centroids to avoid relying on any hidden mutable state.
            TDigest result = TDigest.createAvlTreeDigest(arrays(), compression);
            for (Centroid centroid : digest.centroids()) {
                result.add(centroid.mean(), centroid.count());
            }
            return result;
        }
    }

    private static final class CentroidListBackedTDigest extends AbstractCentroidBackedTDigest {
        private final List<Centroid> centroids;

        private CentroidListBackedTDigest(List<Centroid> centroids) {
            this.centroids = centroids;
        }

        @Override
        protected CentroidIterator centroidIterator() {
            return new CentroidIterator() {
                private int index = -1;

                @Override
                public boolean next() {
                    index++;
                    return index < centroids.size();
                }

                @Override
                public long currentCount() {
                    return centroids.get(index).count();
                }

                @Override
                public double currentMean() {
                    return centroids.get(index).mean();
                }

                @Override
                public boolean hasNext() {
                    return index + 1 < centroids.size();
                }
            };
        }

        @Override
        public long size() {
            long total = 0L;
            for (Centroid centroid : centroids) {
                total += centroid.count();
            }
            return total;
        }

        @Override
        public int centroidCount() {
            return centroids.size();
        }

        @Override
        public Collection<Centroid> centroids() {
            return centroids;
        }

        @Override
        public double getMin() {
            if (centroids.isEmpty()) {
                return Double.NaN;
            }
            return centroids.get(0).mean();
        }

        @Override
        public double getMax() {
            if (centroids.isEmpty()) {
                return Double.NaN;
            }
            return centroids.get(centroids.size() - 1).mean();
        }
    }
}
