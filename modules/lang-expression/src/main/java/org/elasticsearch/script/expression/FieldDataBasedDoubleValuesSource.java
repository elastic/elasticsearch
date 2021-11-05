/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.util.Objects;

abstract class FieldDataBasedDoubleValuesSource extends DoubleValuesSource {

    FieldDataBasedDoubleValuesSource(IndexFieldData<?> fieldData) {
        this.fieldData = Objects.requireNonNull(fieldData);
    }

    protected final IndexFieldData<?> fieldData;

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) {
        return this;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, fieldData.getFieldName());
    }

}
