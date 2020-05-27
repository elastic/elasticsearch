/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.List;

public class Criterion {

    private final SearchSourceBuilder searchSource;
    private final List<HitExtractor> keyExtractors;
    private final HitExtractor timestampExtractor;

    public Criterion(SearchSourceBuilder searchSource, List<HitExtractor> searchAfterExractors, HitExtractor timestampExtractor) {
        this.searchSource = searchSource;
        this.keyExtractors = searchAfterExractors;
        this.timestampExtractor = timestampExtractor;
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

    public void fromTimestamp(long timestampMarker) {
        // TODO: this is likely to be rewritten afterwards
        searchSource.searchAfter(new Object[] { timestampMarker });
    }
}
