/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;

import java.util.BitSet;
import java.util.List;

/**
 * Initial results from a search hit search. Distinct from the following pages
 * because it has a {@link Schema} available. See {@link SearchHitRowSet}
 * for the next pages.
 */
class SchemaSearchHitRowSet extends SearchHitRowSet implements SchemaRowSet {
    private final Schema schema;

    SchemaSearchHitRowSet(Schema schema, List<HitExtractor> exts, BitSet mask, int sizeRequested, int limitHits, SearchResponse response) {
        super(exts, mask, sizeRequested, limitHits, response);
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
