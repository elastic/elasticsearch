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

import org.apache.lucene.search.Filter;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
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
        Filter noMatchFilter = Queries.newMatchAllFilter();
        boolean filterFound = false;
        boolean indicesFound = false;
        boolean currentIndexMatchesIndices = false;
        String filterName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filter".equals(currentFieldName)) {
                    filterFound = true;
                    //TODO We are able to decide whether to parse the filter or not only if indices in the query appears first
                    if (indicesFound && !currentIndexMatchesIndices) {
                        parseContext.parser().skipChildren(); // skip the filter object without parsing it
                    } else {
                        filter = parseContext.parseInnerFilter();
                    }
                } else if ("no_match_filter".equals(currentFieldName)) {
                    if (indicesFound && currentIndexMatchesIndices) {
                        parseContext.parser().skipChildren(); // skip the filter object without parsing it
                    } else {
                        noMatchFilter = parseContext.parseInnerFilter();
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] filter does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("indices".equals(currentFieldName)) {
                    if (indicesFound) {
                        throw  new QueryParsingException(parseContext.index(), "[indices] indices or index already specified");
                    }
                    indicesFound = true;
                    Collection<String> indices = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new QueryParsingException(parseContext.index(), "[indices] no value specified for 'indices' entry");
                        }
                        indices.add(value);
                    }
                    currentIndexMatchesIndices = matchesIndices(parseContext.index().name(), indices.toArray(new String[indices.size()]));
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    if (indicesFound) {
                        throw  new QueryParsingException(parseContext.index(), "[indices] indices or index already specified");
                    }
                    indicesFound = true;
                    currentIndexMatchesIndices = matchesIndices(parseContext.index().name(), parser.text());
                } else if ("no_match_filter".equals(currentFieldName)) {
                    String type = parser.text();
                    if ("all".equals(type)) {
                        noMatchFilter = Queries.newMatchAllFilter();
                    } else if ("none".equals(type)) {
                        noMatchFilter = Queries.newMatchNoDocsFilter();
                    }
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[indices] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!filterFound) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'filter' element");
        }
        if (!indicesFound) {
            throw new QueryParsingException(parseContext.index(), "[indices] requires 'indices' or 'index' element");
        }

        Filter chosenFilter;
        if (currentIndexMatchesIndices) {
            chosenFilter = filter;
        } else {
            chosenFilter = noMatchFilter;
        }

        if (filterName != null) {
            parseContext.addNamedFilter(filterName, chosenFilter);
        }

        return chosenFilter;
    }

    protected boolean matchesIndices(String currentIndex, String... indices) {
        final String[] concreteIndices = clusterService.state().metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), indices);
        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, currentIndex)) {
                return true;
            }
        }
        return false;
    }
}
