/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.search.SortField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.RandomComparator;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class RandomSortParser implements SortParser {

    @Override
    public String[] names() {
        return new String[]{ "_random" };
    }

    @Override
    public SortField parse(XContentParser parser, SearchContext context) throws Exception {
        long seed = System.currentTimeMillis();
        boolean reverse = false;

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    reverse = parser.booleanValue();
                } else if ("order".equals(currentName)) {
                    reverse = "desc".equals(parser.text());
                } else if ("seed".equals(currentName)) {
                    seed = parser.longValue();
                }
            }
        }

        return new SortField("_random", RandomComparator.comparatorSource(seed, context.shardTarget()), reverse);
    }

    @Override
    public SortField createDefault(SearchContext context) {
        return new SortField("_random", RandomComparator.comparatorSource(System.currentTimeMillis(), context.shardTarget()));
    }
}
