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
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.cache.query.terms.TermsLookup;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TermsQueryParser implements QueryParser {

    public static final String NAME = "terms";
    private static final ParseField MIN_SHOULD_MATCH_FIELD = new ParseField("min_match", "min_should_match").withAllDeprecated("Use [bool] query instead");
    private static final ParseField DISABLE_COORD_FIELD = new ParseField("disable_coord").withAllDeprecated("Use [bool] query instead");
    private static final ParseField EXECUTION_FIELD = new ParseField("execution").withAllDeprecated("execution is deprecated and has no effect");
    private Client client;

    @Inject
    public TermsQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "in"};
    }

    @Inject(optional = true)
    public void setClient(Client client) {
        this.client = client;
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String queryName = null;
        String currentFieldName = null;

        String lookupIndex = parseContext.index().name();
        String lookupType = null;
        String lookupId = null;
        String lookupPath = null;
        String lookupRouting = null;
        String minShouldMatch = null;

        boolean disableCoord = false;

        XContentParser.Token token;
        List<Object> terms = new ArrayList<>();
        String fieldName = null;
        float boost = 1f;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_ARRAY) {
                if  (fieldName != null) {
                    throw new QueryParsingException(parseContext, "[terms] query does not support multiple fields");
                }
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Object value = parser.objectBytes();
                    if (value == null) {
                        throw new QueryParsingException(parseContext, "No value specified for terms query");
                    }
                    terms.add(value);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("index".equals(currentFieldName)) {
                            lookupIndex = parser.text();
                        } else if ("type".equals(currentFieldName)) {
                            lookupType = parser.text();
                        } else if ("id".equals(currentFieldName)) {
                            lookupId = parser.text();
                        } else if ("path".equals(currentFieldName)) {
                            lookupPath = parser.text();
                        } else if ("routing".equals(currentFieldName)) {
                            lookupRouting = parser.textOrNull();
                        } else {
                            throw new QueryParsingException(parseContext, "[terms] query does not support [" + currentFieldName
                                    + "] within lookup element");
                        }
                    }
                }
                if (lookupType == null) {
                    throw new QueryParsingException(parseContext, "[terms] query lookup element requires specifying the type");
                }
                if (lookupId == null) {
                    throw new QueryParsingException(parseContext, "[terms] query lookup element requires specifying the id");
                }
                if (lookupPath == null) {
                    throw new QueryParsingException(parseContext, "[terms] query lookup element requires specifying the path");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, EXECUTION_FIELD)) {
                    // ignore
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MIN_SHOULD_MATCH_FIELD)) {
                    if (minShouldMatch != null) {
                        throw new IllegalArgumentException("[" + currentFieldName + "] is not allowed in a filter context for the [" + NAME + "] query");
                    }
                    minShouldMatch = parser.textOrNull();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, DISABLE_COORD_FIELD)) {
                    disableCoord = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[terms] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext, "terms query requires a field name, followed by array of terms");
        }

        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType != null) {
            fieldName = fieldType.names().indexName();
        }

        if (lookupId != null) {
            final TermsLookup lookup = new TermsLookup(lookupIndex, lookupType, lookupId, lookupRouting, lookupPath, parseContext);
            GetRequest getRequest = new GetRequest(lookup.getIndex(), lookup.getType(), lookup.getId()).preference("_local").routing(lookup.getRouting());
            getRequest.copyContextAndHeadersFrom(SearchContext.current());
            final GetResponse getResponse = client.get(getRequest).actionGet();
            if (getResponse.isExists()) {
                List<Object> values = XContentMapValues.extractRawValues(lookup.getPath(), getResponse.getSourceAsMap());
                terms.addAll(values);
            }
        }

        if (terms.isEmpty()) {
            return Queries.newMatchNoDocsQuery();
        }

        Query query;
        if (parseContext.isFilter()) {
            if (fieldType != null) {
                query = fieldType.termsQuery(terms, parseContext);
            } else {
                BytesRef[] filterValues = new BytesRef[terms.size()];
                for (int i = 0; i < filterValues.length; i++) {
                    filterValues[i] = BytesRefs.toBytesRef(terms.get(i));
                }
                query = new TermsQuery(fieldName, filterValues);
            }
        } else {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.setDisableCoord(disableCoord);
            for (Object term : terms) {
                if (fieldType != null) {
                    bq.add(fieldType.termQuery(term, parseContext), Occur.SHOULD);
                } else {
                    bq.add(new TermQuery(new Term(fieldName, BytesRefs.toBytesRef(term))), Occur.SHOULD);
                }
            }
            query = Queries.applyMinimumShouldMatch(bq.build(), minShouldMatch);
        }
        query.setBoost(boost);

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
