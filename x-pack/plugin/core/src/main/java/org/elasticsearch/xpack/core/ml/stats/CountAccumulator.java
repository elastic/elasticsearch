/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An accumulator for simple counts where statistical measures
 * are not of interest.
 */
public class CountAccumulator implements Writeable {

    private Map<String, Long> counts;

    public CountAccumulator() {
        this.counts = new HashMap<String, Long>();
    }

    private CountAccumulator(Map<String, Long> counts) {
        this.counts = counts;
    }

    public CountAccumulator(StreamInput in) throws IOException {
        this.counts = in.readMap(StreamInput::readString, StreamInput::readLong);
    }

    public void merge(CountAccumulator other) {
        counts = Stream.of(counts, other.counts).flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (x, y) -> x + y));
    }

    public void add(String key, Long count) {
        counts.put(key, counts.getOrDefault(key, 0L) + count);
    }

    public Map<String, Long> asMap() {
        return counts;
    }

    public static CountAccumulator fromTermsAggregation(StringTerms termsAggregation) {
        return new CountAccumulator(termsAggregation.getBuckets().stream()
                .collect(Collectors.toMap(bucket -> bucket.getKeyAsString(), bucket -> bucket.getDocCount())));
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(counts, StreamOutput::writeString, StreamOutput::writeLong);
    }

    @Override
    public int hashCode() {
        return Objects.hash(counts);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        CountAccumulator other = (CountAccumulator) obj;
        return Objects.equals(counts, other.counts);
    }
}
