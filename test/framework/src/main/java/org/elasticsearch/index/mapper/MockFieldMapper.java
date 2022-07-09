/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;

// this sucks how much must be overridden just do get a dummy field mapper...
public class MockFieldMapper extends FieldMapper {

    public MockFieldMapper(String fullName) {
        this(new MappedField<>(fullName, new FakeFieldType()));
    }

    public MockFieldMapper(MappedField mappedField) {
        this(findSimpleName(mappedField.name()), mappedField, MultiFields.empty(), CopyTo.empty());
    }

    public MockFieldMapper(String fullName, MappedField mappedField, MultiFields multifields, CopyTo copyTo) {
        super(findSimpleName(fullName), mappedField, multifields, copyTo, false, null);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName());
    }

    static String findSimpleName(String fullName) {
        int ndx = fullName.lastIndexOf('.');
        return fullName.substring(ndx + 1);
    }

    public static class FakeFieldType extends TermBasedFieldType {
        public FakeFieldType() {
            super(true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return "faketype";
        }

        @Override
        public ValueFetcher valueFetcher(String name, SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected String contentType() {
        return null;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {}

    public static class Builder extends FieldMapper.Builder {
        private final MappedFieldType fieldType;

        protected Builder(String name) {
            super(name);
            this.fieldType = new FakeFieldType();
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return FieldMapper.EMPTY_PARAMETERS;
        }

        public Builder addMultiField(Builder builder) {
            this.multiFieldsBuilder.add(builder);
            return this;
        }

        public Builder copyTo(String field) {
            this.copyTo.add(field);
            return this;
        }

        @Override
        public MockFieldMapper build(MapperBuilderContext context) {
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            return new MockFieldMapper(name(), new MappedField<>(name(), fieldType), multiFields, copyTo.build());
        }
    }
}
