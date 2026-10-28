/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.time.ZoneId;
import java.util.Map;

/**
 * MappedFieldType that throws IllegalArgumentException for query or fetch access
 */
public final class UnsearchableFieldType extends MappedFieldType {

    private final String type;
    private final String reason;

    public UnsearchableFieldType(String name, String type, String reason, Map<String, String> meta) {
        super(name, IndexType.NONE, false, meta);
        this.type = type;
        this.reason = reason;
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        throw new IllegalArgumentException("Cannot fetch values for field [" + name() + "]: " + reason);
    }

    @Override
    public String typeName() {
        return type;
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        throw new IllegalArgumentException("Cannot query field [" + name() + "]: " + reason);
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException("Cannot query field [" + name() + "]: " + reason);
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        throw new IllegalArgumentException("Cannot query field [" + name() + "]: " + reason);
    }
}
