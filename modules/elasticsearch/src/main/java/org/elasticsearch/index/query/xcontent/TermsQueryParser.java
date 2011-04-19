/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.lucene.search.Queries.*;
import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * <pre>
 * "terms" : {
 *  "field_name" : [ "value1", "value2" ]
 *  "minimum_match" : 1
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class TermsQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "terms";

    @Inject public TermsQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "in"}; // allow both "in" and "terms" (since its similar to the "terms" filter)
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        boolean disableCoord = false;
        float boost = 1.0f;
        int minimumNumberShouldMatch = 1;
        List<String> values = newArrayList();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    String value = parser.text();
                    if (value == null) {
                        throw new QueryParsingException(index, "No value specified for terms query");
                    }
                    values.add(value);
                }
            } else if (token.isValue()) {
                if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                    disableCoord = parser.booleanValue();
                } else if ("minimum_match".equals(currentFieldName) || "minimumMatch".equals(currentFieldName)) {
                    minimumNumberShouldMatch = parser.intValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                }
            }
        }

        FieldMapper mapper = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                mapper = smartNameFieldMappers.mapper();
            }
        }

        BooleanQuery query = new BooleanQuery(disableCoord);
        for (String value : values) {
            if (mapper != null) {
                query.add(new BooleanClause(mapper.fieldQuery(value, parseContext), BooleanClause.Occur.SHOULD));
            } else {
                query.add(new TermQuery(new Term(fieldName, value)), BooleanClause.Occur.SHOULD);
            }
        }
        query.setBoost(boost);
        if (minimumNumberShouldMatch != -1) {
            query.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
        }
        return wrapSmartNameQuery(optimizeQuery(fixNegativeQueryIfNeeded(query)), smartNameFieldMappers, parseContext);
    }
}

