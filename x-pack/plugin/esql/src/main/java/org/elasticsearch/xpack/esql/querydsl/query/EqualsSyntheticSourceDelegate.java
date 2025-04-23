/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.BaseTermQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class EqualsSyntheticSourceDelegate extends Query {
    private final String fieldName;
    private final String value;

    public EqualsSyntheticSourceDelegate(Source source, String fieldName, String value) {
        super(source);
        this.fieldName = fieldName;
        this.value = value;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new Builder(fieldName, value);
    }

    @Override
    protected String innerToString() {
        return fieldName + "(delegate):" + value;
    }

    private class Builder extends BaseTermQueryBuilder<Builder> {
        private Builder(String name, String value) {
            super(name, value);
        }

        @Override
        protected org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) {
            TextFieldMapper.TextFieldType ft = (TextFieldMapper.TextFieldType) context.getFieldType(fieldName);
            return ft.syntheticSourceDelegate().termQuery(value, context);
        }

        @Override
        public String getWriteableName() {
            return "equals_synthetic_source_delegate";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            // This is just translated on the data node and not sent over the wire.
            throw new UnsupportedOperationException();
        }
    }
}
