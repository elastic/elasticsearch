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
import org.elasticsearch.xpack.eql.execution.sequence.Ordinal;
import org.elasticsearch.xpack.eql.util.ReversedIterator;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.List;

public class Criterion implements QueryRequest {

    private final SearchSourceBuilder searchSource;
    private final List<HitExtractor> keyExtractors;
    private final HitExtractor timestampExtractor;
    private final HitExtractor tiebreakerExtractor;

    // search after markers
    private Ordinal startMarker;
    private Ordinal stopMarker;
    
    private boolean reverse;

    //TODO: should accept QueryRequest instead of another SearchSourceBuilder
    public Criterion(SearchSourceBuilder searchSource,
                     List<HitExtractor> searchAfterExractors,
                     HitExtractor timestampExtractor,
                     HitExtractor tiebreakerExtractor,
                     boolean reverse) {
        this.searchSource = searchSource;
        this.keyExtractors = searchAfterExractors;
        this.timestampExtractor = timestampExtractor;
        this.tiebreakerExtractor = tiebreakerExtractor;

        this.startMarker = null;
        this.stopMarker = null;
        this.reverse = reverse;
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

    @SuppressWarnings({ "unchecked" })
    public Ordinal ordinal(SearchHit hit) {

        Object ts = timestampExtractor.extract(hit);
        if (ts instanceof Number == false) {
            throw new EqlIllegalArgumentException("Expected timestamp as long but got {}", ts);
        }

        long timestamp = ((Number) ts).longValue();
        Comparable<Object> tiebreaker = null;

        if (tiebreakerExtractor != null) {
            Object tb = tiebreakerExtractor.extract(hit);
            if (tb instanceof Comparable == false) {
                throw new EqlIllegalArgumentException("Expected tiebreaker to be Comparable but got {}", tb);
            }
            tiebreaker = (Comparable<Object>) tb;
        }
        return new Ordinal(timestamp, tiebreaker);
    }

    public void startMarker(Ordinal ordinal) {
        startMarker = ordinal;
    }

    public void stopMarker(Ordinal ordinal) {
        stopMarker = ordinal;
    }

    public Ordinal nextMarker() {
        return startMarker.compareTo(stopMarker) < 1 ? startMarker : stopMarker;
    }

    public Criterion useMarker(Ordinal marker) {
        searchSource.searchAfter(marker.toArray());
        return this;
    }

    public Iterable<SearchHit> iterable(List<SearchHit> hits) {
        return () -> reverse ? new ReversedIterator<>(hits) : hits.iterator();
    }
}