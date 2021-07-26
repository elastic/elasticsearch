/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Collections;

/** Mapper for the doc_count field. */
public class DocCountFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_doc_count";
    public static final String CONTENT_TYPE = "_doc_count";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new DocCountFieldMapper());

    public static final class DocCountFieldType extends MappedFieldType {

        public static final DocCountFieldType INSTANCE = new DocCountFieldType();

        public static final int DEFAULT_VALUE = 1;

        public DocCountFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE,  Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String familyTypeName() {
            return NumberFieldMapper.NumberType.INTEGER.typeName();
        }

        @Override
        public boolean isMetadataField() {
            return true;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new QueryShardException(context, "Field [" + name() + "] of type [" + typeName() + "] does not support exists queries");
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new QueryShardException(context, "Field [" + name() + "] of type [" + typeName() + "] is not searchable");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, DEFAULT_VALUE) {
                @Override
                protected Object parseSourceValue(Object value) {
                    if ("".equals(value)) {
                        return DEFAULT_VALUE;
                    } else {
                        return NumberFieldMapper.NumberType.INTEGER.parse(value, false);
                    }
                }
            };
        }
    }

    private DocCountFieldMapper() {
        super(DocCountFieldType.INSTANCE);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.currentToken(), parser);

        // Check that _doc_count is a single value and not an array
        if (context.doc().getByKey(NAME) != null) {
            throw new IllegalArgumentException("Arrays are not allowed for field [" + fieldType().name() + "].");
        }

        int value = parser.intValue(false);
        if (value <= 0) {
            throw new IllegalArgumentException("Field [" + fieldType().name() + "] must be a positive integer. Value ["
                + value + "] is not allowed.");
        }
        context.doc().addWithKey(NAME, new CustomTermFreqField(NAME, NAME, value));
    }

    @Override
    public DocCountFieldType fieldType() {
        return (DocCountFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
