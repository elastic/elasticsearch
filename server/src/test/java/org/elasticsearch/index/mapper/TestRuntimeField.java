/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.time.ZoneId;
import java.util.Collections;

public class TestRuntimeField extends RuntimeFieldType {

    private final String type;

    public TestRuntimeField(String name, String type) {
        super(name, Collections.emptyMap(), null);
        this.type = type;
    }

    @Override
    public String typeName() {
        return type;
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        return null;
    }

    @Override
    protected Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                               ZoneId timeZone, DateMathParser parser, SearchExecutionContext context) {
        throw new UnsupportedOperationException();
    }
}
