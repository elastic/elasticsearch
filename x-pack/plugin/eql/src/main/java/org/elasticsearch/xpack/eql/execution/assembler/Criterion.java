/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.List;

public class Criterion implements QueryRequest {

    private final SearchSourceBuilder searchSource;
    private final List<HitExtractor> keyExtractors;
    private final HitExtractor timestampExtractor;
    private final HitExtractor tiebreakerExtractor;

    // search after markers
    private Object[] startMarker;
    private Object[] stopMarker;

    //TODO: should accept QueryRequest instead of another SearchSourceBuilder
    public Criterion(SearchSourceBuilder searchSource, List<HitExtractor> searchAfterExractors, HitExtractor timestampExtractor,
                     HitExtractor tiebreakerExtractor) {
        this.searchSource = searchSource;
        this.keyExtractors = searchAfterExractors;
        this.timestampExtractor = timestampExtractor;
        this.tiebreakerExtractor = tiebreakerExtractor;

        this.startMarker = null;
        this.stopMarker = null;
    }

    @Override
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

    public Object[] startMarker() {
        return startMarker;
    }

    public Object[] stopMarker() {
        return stopMarker;
    }

    private Object[] marker(SearchHit hit) {
        long timestamp = timestamp(hit);
        Object tiebreaker = null;
        if (tiebreakerExtractor() != null) {
            tiebreaker = tiebreaker(hit);
        }

        return tiebreaker != null ? new Object[] { timestamp, tiebreaker } : new Object[] { timestamp };
    }

    public void startMarker(SearchHit hit) {
        startMarker = marker(hit);
    }

    public void stopMarker(SearchHit hit) {
        stopMarker = marker(hit);
    }

    public Criterion useMarker(Object[] marker) {
        searchSource.searchAfter(marker);
        return this;
    }
}