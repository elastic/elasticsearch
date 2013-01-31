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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;

import java.io.IOException;

public class NestedFilterParser implements FilterParser {

    public static final String NAME = "nested";

    @Inject
    public NestedFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        boolean queryFound = false;
        Filter filter = null;
        boolean filterFound = false;
        float boost = 1.0f;
        boolean join = true;
        String path = null;
        boolean cache = false;
        CacheKeyFilter.Key cacheKey = null;
        String filterName = null;

        // we need a late binding filter so we can inject a parent nested filter inner nested queries
        NestedQueryParser.LateBindingParentFilter currentParentFilterContext = NestedQueryParser.parentFilterContext.get();

        NestedQueryParser.LateBindingParentFilter usAsParentFilter = new NestedQueryParser.LateBindingParentFilter();
        NestedQueryParser.parentFilterContext.set(usAsParentFilter);

        try {
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        queryFound = true;
                        query = parseContext.parseInnerQuery();
                    } else if ("filter".equals(currentFieldName)) {
                        filterFound = true;
                        filter = parseContext.parseInnerFilter();
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[nested] filter does not support [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if ("join".equals(currentFieldName)) {
                        join = parser.booleanValue();
                    } else if ("path".equals(currentFieldName)) {
                        path = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("_scope".equals(currentFieldName)) {
                        throw new QueryParsingException(parseContext.index(), "the [_scope] support in [nested] filter has been removed, use nested filter as a facet_filter in the relevant facet");
                    } else if ("_name".equals(currentFieldName)) {
                        filterName = parser.text();
                    } else if ("_cache".equals(currentFieldName)) {
                        cache = parser.booleanValue();
                    } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                        cacheKey = new CacheKeyFilter.Key(parser.text());
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[nested] filter does not support [" + currentFieldName + "]");
                    }
                }
            }
            if (!queryFound && !filterFound) {
                throw new QueryParsingException(parseContext.index(), "[nested] requires either 'query' or 'filter' field");
            }
            if (path == null) {
                throw new QueryParsingException(parseContext.index(), "[nested] requires 'path' field");
            }

            if (query == null && filter == null) {
                return null;
            }

            if (filter != null) {
                query = new XConstantScoreQuery(filter);
            }

            query.setBoost(boost);

            MapperService.SmartNameObjectMapper mapper = parseContext.smartObjectMapper(path);
            if (mapper == null) {
                throw new QueryParsingException(parseContext.index(), "[nested] failed to find nested object under path [" + path + "]");
            }
            ObjectMapper objectMapper = mapper.mapper();
            if (objectMapper == null) {
                throw new QueryParsingException(parseContext.index(), "[nested] failed to find nested object under path [" + path + "]");
            }
            if (!objectMapper.nested().isNested()) {
                throw new QueryParsingException(parseContext.index(), "[nested] nested object under path [" + path + "] is not of nested type");
            }

            Filter childFilter = parseContext.cacheFilter(objectMapper.nestedTypeFilter(), null);
            usAsParentFilter.filter = childFilter;
            // wrap the child query to only work on the nested path type
            query = new XFilteredQuery(query, childFilter);

            Filter parentFilter = currentParentFilterContext;
            if (parentFilter == null) {
                parentFilter = NonNestedDocsFilter.INSTANCE;
                // don't do special parent filtering, since we might have same nested mapping on two different types
                //if (mapper.hasDocMapper()) {
                //    // filter based on the type...
                //    parentFilter = mapper.docMapper().typeFilter();
                //}
                parentFilter = parseContext.cacheFilter(parentFilter, null);
            }

            Filter nestedFilter;
            if (join) {
                ToParentBlockJoinQuery joinQuery = new ToParentBlockJoinQuery(query, parentFilter, ScoreMode.None);
                nestedFilter = new QueryWrapperFilter(joinQuery);
            } else {
                nestedFilter = new QueryWrapperFilter(query);
            }

            if (cache) {
                nestedFilter = parseContext.cacheFilter(nestedFilter, cacheKey);
            }
            if (filterName != null) {
                parseContext.addNamedFilter(filterName, nestedFilter);
            }
            return nestedFilter;
        } finally {
            // restore the thread local one...
            NestedQueryParser.parentFilterContext.set(currentParentFilterContext);
        }
    }
}
