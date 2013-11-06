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

import org.apache.lucene.search.Filter;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 */
public class IndicesFilterParser implements FilterParser {

    public static final String NAME = "indices";

    @Nullable
    private final ClusterService clusterService;

    @Inject
    public IndicesFilterParser(@Nullable ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Filter filter = null;
        Filter noMatchFilter = Queries.MATCH_ALL_FILTER;
        Filter chosenFilter = null;
        boolean filterFound = false;
        boolean indicesFound = false;
        boolean matchesConcreteIndices = false;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filter".equals(currentFieldName)) {
                    filterFound = true;
                    if (indicesFound) {
                        // Because we know the indices, we can either skip, or parse and use the query
                        if (matchesConcreteIndices) {
                            filter = parseContext.parseInnerFilter();
                            chosenFilter = filter;
                        } else {
                            parseContext.parser().skipChildren(); // skip the filter object without parsing it into a Filter
                        }
                    } else {
                        // We do not know the indices, we must parse the query
                        filter = parseContext.parseInnerFilter();
                    }
                } else if ("no_match_filter".equals(currentFieldName)) {
                    if (indicesFound) {
                        // Because we know the indices, we can either skip, or parse and use the query
                        if (!matchesConcreteIndices) {
                            noMatchFilter = parseContext.parseInnerFilter();
                            chosenFilter = noMatchFilter;
                        } else {
                            parseContext.parser().skipChildren(); // skip the filter object without parsing it into a Filter
                        }
                    } else {
                        // We do not know the indices, we must parse the query
                        noMatchFilter = parseContext.parseInnerFilter();
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] filter does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("indices".equals(currentFieldName)) {
                    if (indicesFound) {
                        throw  new QueryParsingException(parseContext.index(), "[indices] indices already specified");
                    }
                    indicesFound = true;
                    Collection<String> indices = new ArrayList<String>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new QueryParsingException(parseContext.index(), "No value specified for term filter");
                        }
                        indices.add(value);
                    }
                    matchesConcreteIndices = matchesIndices(parseContext, getConcreteIndices(indices));
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    if (indicesFound) {
                        throw  new QueryParsingException(parseContext.index(), "[indices] indices already specified");
                    }
                    indicesFound = true;
                    matchesConcreteIndices = matchesIndices(parseContext, getConcreteIndices(Arrays.asList(parser.text())));
                } else if ("no_match_filter".equals(currentFieldName)) {
                    String type = parser.text();
                    if ("all".equals(type)) {
                        noMatchFilter = Queries.MATCH_ALL_FILTER;
                    } else if ("none".equals(type)) {
                        noMatchFilter = Queries.MATCH_NO_FILTER;
                    }
                    if (indicesFound) {
                        if (!matchesConcreteIndices) {
                            chosenFilter = noMatchFilter;
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!filterFound) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'filter' element");
        }
        if (!indicesFound) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'indices' element");
        }

        if (chosenFilter == null) {
            // Indices were not provided before we encountered the queries, which we hence parsed
            // We must now make a choice
            if (matchesConcreteIndices) {
                chosenFilter = filter;
            } else {
                chosenFilter = noMatchFilter;
            }
        }

        return chosenFilter;
    }

    protected String[] getConcreteIndices(Collection<String> indices) {
        String[] concreteIndices = indices.toArray(new String[indices.size()]);
        if (clusterService != null) {
            MetaData metaData = clusterService.state().metaData();
            concreteIndices = metaData.concreteIndices(indices.toArray(new String[indices.size()]), IgnoreIndices.MISSING, true);
        }
        return concreteIndices;
    }

    protected boolean matchesIndices(QueryParseContext parseContext, String[] concreteIndices) {
        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, parseContext.index().name())) {
                return true;
            }
        }
        return false;
    }
}
