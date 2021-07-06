/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// this sucks how much must be overridden just do get a dummy field mapper...
public class MockFieldMapper extends FieldMapper {

    public MockFieldMapper(String fullName) {
        this(new FakeFieldType(fullName));
    }

    public MockFieldMapper(MappedFieldType fieldType) {
        this(fieldType, Map.of());
    }

    public MockFieldMapper(MappedFieldType fieldType, Map<String, NamedAnalyzer> indexAnalyzers) {
        super(findSimpleName(fieldType.name()), fieldType, indexAnalyzers,
            MultiFields.empty(), new CopyTo.Builder().build(), false, null);
    }

    public MockFieldMapper(String fullName,
                           MappedFieldType fieldType,
                           MultiFields multifields,
                           CopyTo copyTo) {
        super(findSimpleName(fullName), fieldType, multifields, copyTo);
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
        public FakeFieldType(String name) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return "faketype";
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected String contentType() {
        return null;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
    }

    public static class Builder extends FieldMapper.Builder {
        private final MappedFieldType fieldType;

        protected Builder(String name) {
            super(name);
            this.fieldType = new FakeFieldType(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.emptyList();
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
        public MockFieldMapper build(ContentPath contentPath) {
            MultiFields multiFields = multiFieldsBuilder.build(this, contentPath);
            return new MockFieldMapper(name(), fieldType, multiFields, copyTo.build());
        }
    }
}
