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

package org.elasticsearch.index.query.json;

import com.google.inject.Inject;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SpanOrJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "spanOr";

    @Inject public SpanOrJsonQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        float boost = 1.0f;

        List<SpanQuery> clauses = newArrayList();

        String currentFieldName = null;
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.START_ARRAY) {
                if ("clauses".equals(currentFieldName)) {
                    while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (!(query instanceof SpanQuery)) {
                            throw new QueryParsingException(index, "spanNear [clauses] must be of type span query");
                        }
                        clauses.add((SpanQuery) query);
                    }
                }
            } else {
                if ("boost".equals(currentFieldName)) {
                    boost = jp.getFloatValue();
                }
            }
        }
        if (clauses.isEmpty()) {
            throw new QueryParsingException(index, "spanOr must include [clauses]");
        }

        SpanOrQuery query = new SpanOrQuery(clauses.toArray(new SpanQuery[clauses.size()]));
        query.setBoost(boost);
        return query;
    }
}