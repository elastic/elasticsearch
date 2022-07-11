/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

public final class TestRuntimeField implements RuntimeField {

    public static final String CONTENT_TYPE = "test-composite";

    private final String name;
    private final Collection<MappedField> subfields;

    public TestRuntimeField(String name, String type) {
        this(name, Collections.singleton(new MappedField(name, new TestRuntimeFieldType(type))));
    }

    public TestRuntimeField(String name, Collection<MappedField> subfields) {
        this.name = name;
        this.subfields = subfields;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Stream<MappedField> asMappedFields() {
        return subfields.stream();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        builder.endObject();
        return builder;
    }

    public static class TestRuntimeFieldType extends MappedFieldType {
        private final String type;

        public TestRuntimeFieldType(String type) {
            super(false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
            this.type = type;
        }

        @Override
        public ValueFetcher valueFetcher(String name, SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String typeName() {
            return type;
        }

        @Override
        public Query termQuery(String name, Object value, SearchExecutionContext context) {
            return null;
        }

        @Override
        public boolean isSearchable() {
            return true;
        }

        @Override
        public boolean isAggregatable(String name) {
            return true;
        }
    }
}
