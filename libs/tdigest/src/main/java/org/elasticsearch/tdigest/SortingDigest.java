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
 */

package org.elasticsearch.tdigest;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Simple implementation of the TDigest interface that stores internally and sorts all samples to calculate quantiles and CDFs.
 * It provides perfect quantile and cdf calculations and matches or exceeds the performance of MergingDigest at least for millions of
 * samples, at the expense of allocating much more memory.
 */
public class SortingDigest extends TDigest {

    // Tracks all samples. Gets sorted on quantile and cdf calls.
    final ArrayList<Double> values = new ArrayList<>();

    // Indicates if all values have been sorted.
    private boolean isSorted = true;

    @Override
    public void add(double x, int w) {
        checkValue(x);
        for (int i = 0; i < w; i++) {
            values.add(x);
        }
        isSorted = false;
        max = Math.max(max, x);
        min = Math.min(min, x);
    }

    @Override
    public void add(List<? extends TDigest> others) {
        int valuesToAddCount = 0;
        for (TDigest other : others) {
            valuesToAddCount += other.size();
        }
        values.ensureCapacity(valuesToAddCount + values.size());

        for (TDigest other : others) {
            for (Centroid centroid : other.centroids()) {
                add(centroid.mean(), centroid.count());
            }
        }
    }

    @Override
    public void compress() {
        if (isSorted == false) {
            Collections.sort(values);
            isSorted = true;
        }
    }

    @Override
    public long size() {
        return values.size();
    }

    @Override
    public double cdf(double x) {
        compress();
        return Dist.cdf(x, values);
    }

    @Override
    public double quantile(double q) {
        compress();
        return Dist.quantile(q, values);
    }

    @Override
    public Collection<Centroid> centroids() {
        compress();

        return new AbstractCollection<>() {
            @Override
            public Iterator<Centroid> iterator() {
                return new Iterator<>() {
                    int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < values.size();
                    }

                    @Override
                    public Centroid next() {
                        Centroid rc = new Centroid(values.get(i), 1);
                        i++;
                        return rc;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Default operation");
                    }
                };
            }

            @Override
            public int size() {
                return values.size();
            }
        };
    }

    @Override
    public double compression() {
        return 1;
    }

    @Override
    public void add(TDigest other) {
        add(List.of(other));
    }

    @Override
    public int centroidCount() {
        return centroids().size();
    }

    /**
     * Returns an upper bound on the number bytes that will be required to represent this histogram.
     */
    @Override
    public int byteSize() {
        return values.size() * 8;
    }
}
