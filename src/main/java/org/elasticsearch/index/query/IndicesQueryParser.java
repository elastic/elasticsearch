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

import org.apache.lucene.search.Query;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

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
        Query noMatchQuery = Queries.newMatchAllQuery();
        boolean queryFound = false;
        boolean indicesFound = false;
        boolean currentIndexMatchesIndices = false;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    //TODO We are able to decide whether to parse the query or not only if indices in the query appears first
                    queryFound = true;
                    if (indicesFound && !currentIndexMatchesIndices) {
                        parseContext.parser().skipChildren(); // skip the query object without parsing it
                    } else {
                        query = parseContext.parseInnerQuery();
                    }
                } else if ("no_match_query".equals(currentFieldName)) {
                    if (indicesFound && currentIndexMatchesIndices) {
                        parseContext.parser().skipChildren(); // skip the query object without parsing it
                    } else {
                        noMatchQuery = parseContext.parseInnerQuery();
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("indices".equals(currentFieldName)) {
                    if (indicesFound) {
                        throw  new QueryParsingException(parseContext.index(), "[indices] indices or index already specified");
                    }
                    indicesFound = true;
                    Collection<String> indices = new ArrayList<String>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new QueryParsingException(parseContext.index(), "[indices] no value specified for 'indices' entry");
                        }
                        indices.add(value);
                    }
                    currentIndexMatchesIndices = matchesIndices(parseContext.index().name(), indices.toArray(new String[indices.size()]));
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    if (indicesFound) {
                        throw  new QueryParsingException(parseContext.index(), "[indices] indices or index already specified");
                    }
                    indicesFound = true;
                    currentIndexMatchesIndices = matchesIndices(parseContext.index().name(), parser.text());
                } else if ("no_match_query".equals(currentFieldName)) {
                    String type = parser.text();
                    if ("all".equals(type)) {
                        noMatchQuery = Queries.newMatchAllQuery();
                    } else if ("none".equals(type)) {
                        noMatchQuery = MatchNoDocsQuery.INSTANCE;
                    }
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'query' element");
        }
        if (!indicesFound) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'indices' or 'index' element");
        }

        Query chosenQuery;
        if (currentIndexMatchesIndices) {
            chosenQuery = query;
        } else {
            chosenQuery = noMatchQuery;
        }
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, chosenQuery);
        }
        return chosenQuery;
    }

    protected boolean matchesIndices(String currentIndex, String... indices) {
        final String[] concreteIndices;
        try {
            concreteIndices = clusterService.state().metaData().concreteIndices(indices, IgnoreIndices.MISSING, true);
        } catch(IndexMissingException e) {
            //Although we use IgnoreIndices.MISSING, according to MetaData#concreteIndices contract,
            // we get IndexMissing either when we have a single index that is missing or when all indices are missing
            return false;
        }

        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, currentIndex)) {
                return true;
            }
        }
        return false;
    }
}
