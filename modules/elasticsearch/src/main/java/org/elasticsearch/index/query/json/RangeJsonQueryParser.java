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
import org.apache.lucene.search.TermRangeQuery;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RangeJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "range";

    @Inject public RangeJsonQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        JsonToken token = jp.getCurrentToken();
        if (token == JsonToken.START_OBJECT) {
            token = jp.nextToken();
        }
        assert token == JsonToken.FIELD_NAME;
        String fieldName = jp.getCurrentName();

        String from = null;
        String to = null;
        boolean includeLower = true;
        boolean includeUpper = true;
        float boost = 1.0f;

        String currentFieldName = null;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else {
                if ("from".equals(currentFieldName)) {
                    if (jp.getCurrentToken() == JsonToken.VALUE_NULL) {
                        from = null;
                    } else {
                        from = jp.getText();
                    }
                } else if ("to".equals(currentFieldName)) {
                    if (jp.getCurrentToken() == JsonToken.VALUE_NULL) {
                        to = null;
                    } else {
                        to = jp.getText();
                    }
                } else if ("includeLower".equals(currentFieldName)) {
                    includeLower = token == JsonToken.VALUE_TRUE;
                } else if ("includeUpper".equals(currentFieldName)) {
                    includeUpper = token == JsonToken.VALUE_TRUE;
                } else if ("boost".equals(currentFieldName)) {
                    boost = jp.getFloatValue();
                }
            }
        }

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            FieldMapper fieldMapper = smartNameFieldMappers.fieldMappers().mapper();
            if (fieldMapper != null) {
                query = fieldMapper.rangeQuery(from, to, includeLower, includeUpper);
            }
        }
        if (query == null) {
            query = new TermRangeQuery(fieldName, from, to, includeLower, includeUpper);
        }
        query.setBoost(boost);
        return wrapSmartNameQuery(query, smartNameFieldMappers, parseContext.filterCache());
    }
}