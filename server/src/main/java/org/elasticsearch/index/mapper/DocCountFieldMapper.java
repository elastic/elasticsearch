/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;

/** Mapper for the doc_count field. */
public class DocCountFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_doc_count";
    public static final String CONTENT_TYPE = "_doc_count";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new DocCountFieldMapper());

    public static final class DocCountFieldType extends MappedFieldType {

        public static final DocCountFieldType INSTANCE = new DocCountFieldType();

        private static final Long defaultValue = 1L;

        public DocCountFieldType() {
            super(NAME, false, false, true, TextSearchInfo.NONE,  Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String familyTypeName() {
            return NumberFieldMapper.NumberType.LONG.typeName();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(NAME);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Field [" + name() + "] of type [" + typeName() + "] is not searchable");
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, defaultValue) {
                @Override
                protected Object parseSourceValue(Object value) {
                    if ("".equals(value)) {
                        return defaultValue;
                    } else {
                        return NumberFieldMapper.NumberType.objectToLong(value, false);
                    }
                }
            };
        }
    }

    private DocCountFieldMapper() {
        super(DocCountFieldType.INSTANCE);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.currentToken(), parser);

        long value = parser.longValue(false);
        if (value <= 0) {
            throw new IllegalArgumentException("Field [" + fieldType().name() + "] must be a positive integer.");
        }
        final Field docCount = new NumericDocValuesField(NAME, value);
        context.doc().add(docCount);
    }

    @Override
    public void preParse(ParseContext context) { }

    @Override
    public DocCountFieldType fieldType() {
        return (DocCountFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
