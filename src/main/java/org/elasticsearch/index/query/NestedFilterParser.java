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
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;

import java.io.IOException;

public class NestedFilterParser implements FilterParser {

    public static final String NAME = "nested";

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public NestedFilterParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        final NestedQueryParser.ToBlockJoinQueryBuilder builder = new NestedQueryParser.ToBlockJoinQueryBuilder(parseContext);

        float boost = 1.0f;
        boolean cache = false;
        HashedBytesRef cacheKey = null;
        String filterName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    builder.query();
                } else if ("filter".equals(currentFieldName)) {
                    builder.filter();
                } else if ("inner_hits".equals(currentFieldName)) {
                    builder.setInnerHits(innerHitsQueryParserHelper.parse(parseContext));
                } else {
                    throw new QueryParsingException(parseContext.index(), "[nested] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("path".equals(currentFieldName)) {
                    builder.setPath(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[nested] filter does not support [" + currentFieldName + "]");
                }
            }
        }
        builder.setScoreMode(ScoreMode.None);
        ToParentBlockJoinQuery joinQuery = builder.build();
        if (joinQuery != null) {
            joinQuery.getChildQuery().setBoost(boost);
            Filter nestedFilter = Queries.wrap(joinQuery, parseContext);
            if (cache) {
                nestedFilter = parseContext.cacheFilter(nestedFilter, cacheKey, parseContext.autoFilterCachePolicy());
            }
            if (filterName != null) {
                parseContext.addNamedFilter(filterName, nestedFilter);
            }
            return nestedFilter;
        } else {
            return null;
        }
    }
}
