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

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Optional;

/**
 * A Span Query that matches documents containing a term.
 * @see SpanTermQuery
 */
public class SpanTermQueryBuilder extends BaseTermQueryBuilder<SpanTermQueryBuilder> implements SpanQueryBuilder {
    public static final String NAME = "span_term";

    private static final ParseField TERM_FIELD = new ParseField("term");

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, String) */
    public SpanTermQueryBuilder(String name, String value) {
        super(name, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, int) */
    public SpanTermQueryBuilder(String name, int value) {
        super(name, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, long) */
    public SpanTermQueryBuilder(String name, long value) {
        super(name, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, float) */
    public SpanTermQueryBuilder(String name, float value) {
        super(name, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, double) */
    public SpanTermQueryBuilder(String name, double value) {
        super(name, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, Object) */
    public SpanTermQueryBuilder(String name, Object value) {
        super(name, value);
    }

    /**
     * Read from a stream.
     */
    public SpanTermQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected SpanQuery doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType mapper = context.fieldMapper(fieldName);
        Term term;
        if (mapper == null) {
            term = new Term(fieldName, BytesRefs.toBytesRef(value));
        } else {
            Query termQuery = mapper.termQuery(value, context);
            term = MappedFieldType.extractTerm(termQuery);
        }
        return new SpanTermQuery(term);
    }

    public static Optional<SpanTermQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(), "[span_term] query doesn't support multiple fields, found ["
                            + fieldName + "] and [" + currentFieldName + "]");
                }
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (parseContext.getParseFieldMatcher().match(currentFieldName, TERM_FIELD)) {
                            value = parser.objectBytes();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, BaseTermQueryBuilder.VALUE_FIELD)) {
                            value = parser.objectBytes();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                            boost = parser.floatValue();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[span_term] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                fieldName = parser.currentName();
                value = parser.objectBytes();
            }
        }

        SpanTermQueryBuilder result = new SpanTermQueryBuilder(fieldName, value);
        result.boost(boost).queryName(queryName);
        return Optional.of(result);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
