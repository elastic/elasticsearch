/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

/**
 * A class that encapsulates a minimum and a maximum {@link Comparable}.
 */
public class MinAndMax implements Writeable {
    private final Comparable minValue;
    private final Comparable maxValue;

    public MinAndMax(Comparable minValue, Comparable maxValue) {
        this.minValue = Objects.requireNonNull(minValue);
        this.maxValue = Objects.requireNonNull(maxValue);
    }

    public MinAndMax(StreamInput in) throws IOException {
        this.minValue = Lucene.readSortValue(in);
        this.maxValue = Lucene.readSortValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Lucene.writeSortValue(out, minValue);
        Lucene.writeSortValue(out, maxValue);
    }

    /**
     * Return the minimum value.
     */
    public Comparable getMin() {
        return minValue;
    }

    /**
     * Return the maximum value.
     */
    public Comparable getMax() {
        return maxValue;
    }

    /**
     * Return a {@link Comparator} for {@link MinAndMax} values according to the provided {@link SortOrder}.
     */
    public static Comparator<MinAndMax> getComparator(SortOrder order) {
        final Comparator<MinAndMax> cmp = (a, b) -> order == SortOrder.ASC  ?
            final Comparator<MinAndMax> cmp = order == SortOrder.ASC  ?
                  Comparator.comparing(MinAndMax::getMin) :  Comparator.comparing(MinAndMax::getMax);
        if  (order == SortOrder.DESC) {
            return cmp.reversed();
        }
        return cmp;
    }
}
