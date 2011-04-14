/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DeletionAwareConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ConstantScoreQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "constant_score";

    @Inject public ConstantScoreQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Filter filter = null;
        Query query = null;
        float boost = 1.0f;
        boolean cache = false;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filter".equals(currentFieldName)) {
                    filter = parseContext.parseInnerFilter();
                } else if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                }
            } else if (token.isValue()) {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                }
            }
        }
        if (filter == null && query == null) {
            throw new QueryParsingException(index, "[constant_score] requires either 'filter' or 'query' element");
        }

        if (filter != null) {
            // cache the filter if possible needed
            if (cache) {
                filter = parseContext.cacheFilter(filter);
            }

            Query query1 = new DeletionAwareConstantScoreQuery(filter);
            query1.setBoost(boost);
            return query1;
        }
        // Query
        query = new ConstantScoreQuery(query);
        query.setBoost(boost);
        return query;
    }
}