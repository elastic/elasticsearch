/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.type.Schema;

import java.util.BitSet;
import java.util.List;

/**
 * Initial results from a scroll search. Distinct from the following pages
 * because it has a {@link Schema} available. See {@link SearchHitRowSet}
 * for the next pages.
 */
class SchemaSearchHitRowSet extends SearchHitRowSet implements SchemaRowSet {
    private final Schema schema;

    SchemaSearchHitRowSet(Schema schema, List<HitExtractor> exts, BitSet mask, int limitHits, SearchResponse response) {
        super(exts, mask, limitHits, response);
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
