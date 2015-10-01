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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.io.IOException;
import java.util.Collection;

/**
 *
 */
public class MissingQueryParser implements QueryParser {

    public static final String NAME = "missing";
    public static final boolean DEFAULT_NULL_VALUE = false;
    public static final boolean DEFAULT_EXISTENCE_VALUE = true;

    @Inject
    public MissingQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldPattern = null;
        String queryName = null;
        boolean nullValue = DEFAULT_NULL_VALUE;
        boolean existence = DEFAULT_EXISTENCE_VALUE;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    fieldPattern = parser.text();
                } else if ("null_value".equals(currentFieldName)) {
                    nullValue = parser.booleanValue();
                } else if ("existence".equals(currentFieldName)) {
                    existence = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[missing] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (fieldPattern == null) {
            throw new QueryParsingException(parseContext, "missing must be provided with a [field]");
        }

        return newFilter(parseContext, fieldPattern, existence, nullValue, queryName);
    }

    public static Query newFilter(QueryParseContext parseContext, String fieldPattern, boolean existence, boolean nullValue, String queryName) {
        if (!existence && !nullValue) {
            throw new QueryParsingException(parseContext, "missing must have either existence, or null_value, or both set to true");
        }

        final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldMapper.FieldNamesFieldType)parseContext.mapperService().fullName(FieldNamesFieldMapper.NAME);
        if (fieldNamesFieldType == null) {
            // can only happen when no types exist, so no docs exist either
            return Queries.newMatchNoDocsQuery();
        }

        ObjectMapper objectMapper = parseContext.getObjectMapper(fieldPattern);
        if (objectMapper != null) {
            // automatic make the object mapper pattern
            fieldPattern = fieldPattern + ".*";
        }

        Collection<String> fields = parseContext.simpleMatchToIndexNames(fieldPattern);
        if (fields.isEmpty()) {
            if (existence) {
                // if we ask for existence of fields, and we found none, then we should match on all
                return Queries.newMatchAllQuery();
            }
            return null;
        }

        Query existenceFilter = null;
        Query nullFilter = null;

        if (existence) {
            BooleanQuery.Builder boolFilter = new BooleanQuery.Builder();
            for (String field : fields) {
                MappedFieldType fieldType = parseContext.fieldMapper(field);
                Query filter = null;
                if (fieldNamesFieldType.isEnabled()) {
                    final String f;
                    if (fieldType != null) {
                        f = fieldType.names().indexName();
                    } else {
                        f = field;
                    }
                    filter = fieldNamesFieldType.termQuery(f, parseContext);
                }
                // if _field_names are not indexed, we need to go the slow way
                if (filter == null && fieldType != null) {
                    filter = fieldType.rangeQuery(null, null, true, true);
                }
                if (filter == null) {
                    filter = new TermRangeQuery(field, null, null, true, true);
                }
                boolFilter.add(filter, BooleanClause.Occur.SHOULD);
            }

            existenceFilter = boolFilter.build();
            existenceFilter = Queries.not(existenceFilter);;
        }

        if (nullValue) {
            for (String field : fields) {
                MappedFieldType fieldType = parseContext.fieldMapper(field);
                if (fieldType != null) {
                    nullFilter = fieldType.nullValueQuery();
                }
            }
        }

        Query filter;
        if (nullFilter != null) {
            if (existenceFilter != null) {
                filter = new BooleanQuery.Builder()
                    .add(existenceFilter, BooleanClause.Occur.SHOULD)
                    .add(nullFilter, BooleanClause.Occur.SHOULD)
                    .build();
            } else {
                filter = nullFilter;
            }
        } else {
            filter = existenceFilter;
        }

        if (filter == null) {
            return null;
        }

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, existenceFilter);
        }
        return new ConstantScoreQuery(filter);
    }
}
