/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Quantile estimation removed from DataFu library class StreamingQuantile for comparison.
 *
 * Computes approximate quantiles for a (not necessarily sorted) input bag, using the Munro-Paterson algorithm.
 *
 * The algorithm is described here: http://www.cs.ucsb.edu/~suri/cs290/MunroPat.pdf
 *
 * The implementation is based on the one in Sawzall, available here: szlquantile.cc
 */
public class QuantileEstimator {
    private static final long MAX_TOT_ELEMS = 1024L * 1024L * 1024L * 1024L;

    private final List<List<Double>> buffer = new ArrayList<>();
    private final int numQuantiles;
    private final int maxElementsPerBuffer;
    private int totalElements;
    private double min;
    private double max;

    @SuppressWarnings("WeakerAccess")
    public QuantileEstimator(int numQuantiles) {
        this.numQuantiles = numQuantiles;
        this.maxElementsPerBuffer = computeMaxElementsPerBuffer();
    }

    private int computeMaxElementsPerBuffer() {
        double epsilon = 1.0 / (numQuantiles - 1.0);
        int b = 2;
        while ((b - 2) * (0x1L << (b - 2)) + 0.5 <= epsilon * MAX_TOT_ELEMS) {
            ++b;
        }
        return (int) (MAX_TOT_ELEMS / (0x1L << (b - 1)));
    }

    private void ensureBuffer(int level) {
        while (buffer.size() < level + 1) {
            buffer.add(null);
        }
        if (buffer.get(level) == null) {
            buffer.set(level, new ArrayList<Double>());
        }
    }

    private void collapse(List<Double> a, List<Double> b, List<Double> out) {
        if (a == null || b == null || out == null) {
            return;
        }
        int indexA = 0, indexB = 0, count = 0;
        Double smaller;
        while (indexA < maxElementsPerBuffer || indexB < maxElementsPerBuffer) {
            if (indexA >= maxElementsPerBuffer ||
                    (indexB < maxElementsPerBuffer && a.get(indexA) >= b.get(indexB))) {
                smaller = b.get(indexB++);
            } else {
                smaller = a.get(indexA++);
            }

            if (count++ % 2 == 0) {
                out.add(smaller);
            }
        }
        a.clear();
        b.clear();
    }

    private void recursiveCollapse(List<Double> buf, int level) {
        ensureBuffer(level + 1);

        List<Double> merged;
        if (buffer.get(level + 1).isEmpty()) {
            merged = buffer.get(level + 1);
        } else {
            merged = new ArrayList<>(maxElementsPerBuffer);
        }

        collapse(buffer.get(level), buf, merged);
        if (buffer.get(level + 1) != merged) {
            recursiveCollapse(merged, level + 1);
        }
    }

    public void add(double elem) {
        if (totalElements == 0 || elem < min) {
            min = elem;
        }
        if (totalElements == 0 || max < elem) {
            max = elem;
        }

        if (totalElements > 0 && totalElements % (2 * maxElementsPerBuffer) == 0) {
            Collections.sort(buffer.get(0));
            Collections.sort(buffer.get(1));
            recursiveCollapse(buffer.get(0), 1);
        }

        ensureBuffer(0);
        ensureBuffer(1);
        int index = buffer.get(0).size() < maxElementsPerBuffer ? 0 : 1;
        buffer.get(index).add(elem);
        totalElements++;
    }

    public void clear() {
        buffer.clear();
        totalElements = 0;
    }

    @SuppressWarnings("WeakerAccess")
    public List<Double> getQuantiles() {
        List<Double> quantiles = new ArrayList<>();
        quantiles.add(min);

        if (buffer.get(0) != null) {
            Collections.sort(buffer.get(0));
        }
        if (buffer.get(1) != null) {
            Collections.sort(buffer.get(1));
        }

        int[] index = new int[buffer.size()];
        long S = 0;
        for (int i = 1; i <= numQuantiles - 2; i++) {
            long targetS = (long) Math.ceil(i * (totalElements / (numQuantiles - 1.0)));

            while (true) {
                double smallest = max;
                int minBufferId = -1;
                for (int j = 0; j < buffer.size(); j++) {
                    if (buffer.get(j) != null && index[j] < buffer.get(j).size()) {
                        if (smallest < buffer.get(j).get(index[j]) == false) {
                            smallest = buffer.get(j).get(index[j]);
                            minBufferId = j;
                        }
                    }
                }

                long incrementS = minBufferId <= 1 ? 1L : (0x1L << (minBufferId - 1));
                if (S + incrementS >= targetS) {
                    quantiles.add(smallest);
                    break;
                } else {
                    index[minBufferId]++;
                    S += incrementS;
                }
            }
        }

        quantiles.add(max);
        return quantiles;
    }

    @SuppressWarnings("WeakerAccess")
    public int serializedSize() {
        int r = 4 + 4 + 4 + 4 + 4;
        for (List<Double> b1 : buffer) {
            r += b1.size();
        }
        return r;
    }
}
