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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class PercolatorQueryParser implements QueryParser<PercolatorQueryBuilder> {

    public static final ParseField QUERY_NAME_FIELD = new ParseField(PercolatorQueryBuilder.NAME);
    public static final ParseField DOCUMENT_FIELD = new ParseField("document");
    public static final ParseField DOCUMENT_TYPE_FIELD = new ParseField("document_type");
    public static final ParseField INDEXED_DOCUMENT_FIELD_INDEX = new ParseField("index");
    public static final ParseField INDEXED_DOCUMENT_FIELD_TYPE = new ParseField("type");
    public static final ParseField INDEXED_DOCUMENT_FIELD_ID = new ParseField("id");
    public static final ParseField INDEXED_DOCUMENT_FIELD_ROUTING = new ParseField("routing");
    public static final ParseField INDEXED_DOCUMENT_FIELD_PREFERENCE = new ParseField("preference");
    public static final ParseField INDEXED_DOCUMENT_FIELD_VERSION = new ParseField("version");

    @Override
    public PercolatorQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String documentType = null;

        String indexedDocumentIndex = null;
        String indexedDocumentType = null;
        String indexedDocumentId = null;
        String indexedDocumentRouting = null;
        String indexedDocumentPreference = null;
        Long indexedDocumentVersion = null;

        BytesReference source = null;

        String queryName = null;
        String currentFieldName = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, DOCUMENT_FIELD)) {
                    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                        builder.copyCurrentStructure(parser);
                        builder.flush();
                        source = builder.bytes();
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + PercolatorQueryBuilder.NAME +
                            "] query does not support [" + token + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, DOCUMENT_TYPE_FIELD)) {
                    documentType = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_INDEX)) {
                    indexedDocumentIndex = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_TYPE)) {
                    indexedDocumentType = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_ID)) {
                    indexedDocumentId = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_ROUTING)) {
                    indexedDocumentRouting = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_PREFERENCE)) {
                    indexedDocumentPreference = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_VERSION)) {
                    indexedDocumentVersion = parser.longValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + PercolatorQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + PercolatorQueryBuilder.NAME +
                        "] query does not support [" + token + "]");
            }
        }

        if (documentType == null) {
            throw new IllegalArgumentException("[" + PercolatorQueryBuilder.NAME + "] query is missing required [" +
                    DOCUMENT_TYPE_FIELD.getPreferredName() + "] parameter");
        }

        PercolatorQueryBuilder queryBuilder;
        if (source != null) {
            queryBuilder = new PercolatorQueryBuilder(documentType, source);
        } else if (indexedDocumentId != null) {
            queryBuilder = new PercolatorQueryBuilder(documentType, indexedDocumentIndex, indexedDocumentType,
                    indexedDocumentId, indexedDocumentRouting, indexedDocumentPreference, indexedDocumentVersion);
        } else {
            throw new IllegalArgumentException("[" + PercolatorQueryBuilder.NAME + "] query, nothing to percolate");
        }
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }

    @Override
    public PercolatorQueryBuilder getBuilderPrototype() {
        return PercolatorQueryBuilder.PROTO;
    }

}
