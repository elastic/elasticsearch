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
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class SpanFirstJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "spanFirst";

    @Inject public SpanFirstJsonQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        float boost = 1.0f;

        SpanQuery match = null;
        int end = -1;

        String currentFieldName = null;
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.START_OBJECT) {
                if ("match".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (!(query instanceof SpanQuery)) {
                        throw new QueryParsingException(index, "spanFirst [match] must be of type span query");
                    }
                    match = (SpanQuery) query;
                }
            } else {
                if ("boost".equals(currentFieldName)) {
                    boost = jp.getFloatValue();
                } else if ("end".equals(currentFieldName)) {
                    end = jp.getIntValue();
                }
            }
        }
        if (match == null) {
            throw new QueryParsingException(index, "spanFirst must have [match] span query clause");
        }
        if (end == -1) {
            throw new QueryParsingException(index, "spanFirst must have [end] set for it");
        }

        SpanFirstQuery query = new SpanFirstQuery(match, end);
        query.setBoost(boost);
        return query;
    }
}