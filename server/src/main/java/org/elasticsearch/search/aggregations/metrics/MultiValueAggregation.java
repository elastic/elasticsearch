/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface MultiValueAggregation extends Aggregation {

    /**
     * Return an iterable over all value names this multi value aggregation provides.
     *
     * The iterable might be created on the fly, if you need to call this multiple times, please
     * cache the result in a variable on caller side..
     *
     * @return iterable over all value names
     */
    Iterable<String> valueNames();

    /**
     * Return a list of all results with the specified name
     *
     * @param name of the value
     * @return list of all values formatted as string
     */
    List<String> getValuesAsStrings(String name);

    /**
     * Configured maximum number of ranked source documents this aggregation may emit per bucket.
     * Dest writers such as transforms use this together with {@link #getRankedHits()} to decide
     * whether to persist a {@code top} array. Default is {@code 1}.
     */
    default int getRankedHitSize() {
        return 1;
    }

    /**
     * Actual ranked hits in sort order. Length may be less than {@link #getRankedHitSize()}
     * when the bucket has fewer matching documents. Default is empty.
     */
    default List<RankedHit> getRankedHits() {
        return List.of();
    }

    /**
     * One ranked source document: sort values and metrics, matching {@code top_metrics} xcontent.
     */
    record RankedHit(List<Object> sort, Map<String, Object> metrics) {
        public RankedHit {
            Objects.requireNonNull(sort, "sort");
            Objects.requireNonNull(metrics, "metrics");
            sort = List.copyOf(sort);
            metrics = Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
        }
    }
}
