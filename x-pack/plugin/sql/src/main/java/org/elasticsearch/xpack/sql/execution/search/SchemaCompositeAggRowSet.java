/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;

import java.util.BitSet;
import java.util.List;

/**
 * Extension of the {@link RowSet} over a composite agg, extending it to provide its schema.
 * Used for the initial response.
 */
class SchemaCompositeAggRowSet extends CompositeAggRowSet implements SchemaRowSet {

    private final Schema schema;

    SchemaCompositeAggRowSet(
        Schema schema,
        List<BucketExtractor> exts,
        BitSet mask,
        SearchResponse r,
        int sizeRequested,
        int limitAggs,
        boolean mightProducePartialPages
    ) {
        super(exts, mask, r, sizeRequested, limitAggs, mightProducePartialPages);
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
