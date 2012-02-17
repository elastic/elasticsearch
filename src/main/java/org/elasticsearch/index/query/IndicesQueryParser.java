/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Sets;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Set;

/**
 */
public class IndicesQueryParser implements QueryParser {

    public static final String NAME = "indices";

    @Nullable
    private final ClusterService clusterService;

    @Inject
    public IndicesQueryParser(@Nullable ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        Set<String> indices = Sets.newHashSet();

        String currentFieldName = null;
        XContentParser.Token token;
        Query noMatchQuery = Queries.MATCH_ALL_QUERY;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else if ("no_match_query".equals(currentFieldName)) {
                    noMatchQuery = parseContext.parseInnerQuery();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("indices".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new QueryParsingException(parseContext.index(), "No value specified for term filter");
                        }
                        indices.add(value);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    indices.add(parser.text());
                } else if ("no_match_query".equals(currentFieldName)) {
                    String type = parser.text();
                    if ("all".equals(type)) {
                        noMatchQuery = Queries.MATCH_ALL_QUERY;
                    } else if ("none".equals(type)) {
                        noMatchQuery = MatchNoDocsQuery.INSTANCE;
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (query == null) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'query' element");
        }
        if (indices.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'indices' element");
        }

        String[] concreteIndices = indices.toArray(new String[indices.size()]);
        if (clusterService != null) {
            MetaData metaData = clusterService.state().metaData();
            concreteIndices = metaData.concreteIndices(indices.toArray(new String[indices.size()]), true, true);
        }

        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, parseContext.index().name())) {
                return query;
            }
        }
        return noMatchQuery;
    }
}
