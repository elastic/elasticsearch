/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.List;

public class Criterion {

    private final SearchSourceBuilder searchSource;
    private final List<HitExtractor> keyExtractors;
    private final HitExtractor timestampExtractor;
    private final HitExtractor tiebreakerExtractor;

    public Criterion(SearchSourceBuilder searchSource, List<HitExtractor> searchAfterExractors, HitExtractor timestampExtractor,
                     HitExtractor tiebreakerExtractor) {
        this.searchSource = searchSource;
        this.keyExtractors = searchAfterExractors;
        this.timestampExtractor = timestampExtractor;
        this.tiebreakerExtractor = tiebreakerExtractor;
    }

    public SearchSourceBuilder searchSource() {
        return searchSource;
    }

    public List<HitExtractor> keyExtractors() {
        return keyExtractors;
    }

    public HitExtractor timestampExtractor() {
        return timestampExtractor;
    }

    public HitExtractor tiebreakerExtractor() {
        return tiebreakerExtractor;
    }

    public long timestamp(SearchHit hit) {
        Object ts = timestampExtractor.extract(hit);
        if (ts instanceof Number) {
            return ((Number) ts).longValue();
        }
        throw new EqlIllegalArgumentException("Expected timestamp as long but got {}", ts);
    }

    @SuppressWarnings({ "unchecked" })
    public Comparable<Object> tiebreaker(SearchHit hit) {
        if (tiebreakerExtractor == null) {
            return null;
        }
        Object tb = tiebreakerExtractor.extract(hit);
        if (tb instanceof Comparable) {
            return (Comparable<Object>) tb;
        }
        throw new EqlIllegalArgumentException("Expected tiebreaker to be Comparable but got {}", tb);
    }

    public void fromMarkers(Object[] markers) {
        // TODO: this is likely to be rewritten afterwards
        searchSource.searchAfter(markers);
    }
}