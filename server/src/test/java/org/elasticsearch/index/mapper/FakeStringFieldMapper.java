/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

// Like a String mapper but with very few options. We just use it to test if highlighting on a custom string mapped field works as expected.
public class FakeStringFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "fake_string";

    public static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    }

    public static class Builder extends FieldMapper.Builder {

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.emptyList();
        }

        @Override
        public FakeStringFieldMapper build(ContentPath contentPath) {
            return new FakeStringFieldMapper(
                new FakeStringFieldType(name, true,
                    new TextSearchInfo(FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER)),
                multiFieldsBuilder.build(this, contentPath), copyTo.build());
        }
    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class FakeStringFieldType extends StringFieldType {

        private FakeStringFieldType(String name, boolean stored, TextSearchInfo textSearchInfo) {
            super(name, true, stored, true, textSearchInfo, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }
    }

    protected FakeStringFieldMapper(MappedFieldType mappedFieldType,
                                    MultiFields multiFields, CopyTo copyTo) {
        super(mappedFieldType.name(), mappedFieldType, Lucene.STANDARD_ANALYZER, multiFields, copyTo);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        String value = context.parser().textOrNull();

        if (value == null) {
            return;
        }

        Field field = new Field(fieldType().name(), value, FIELD_TYPE);
        context.doc().add(field);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
