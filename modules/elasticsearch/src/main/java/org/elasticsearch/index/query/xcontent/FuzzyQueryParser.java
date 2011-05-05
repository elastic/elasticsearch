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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class FuzzyQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "fuzzy";

    @Inject public FuzzyQueryParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String fieldName = parser.currentName();

        String value = null;
        float boost = 1.0f;
        String minSimilarity = "0.5";
        int prefixLength = FuzzyQuery.defaultPrefixLength;
        int maxExpansions = FuzzyQuery.defaultMaxExpansions;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if ("term".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("value".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("min_similarity".equals(currentFieldName) || "minSimilarity".equals(currentFieldName)) {
                        minSimilarity = parser.text();
                    } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                        prefixLength = parser.intValue();
                    } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                        maxExpansions = parser.intValue();
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.text();
            // move to the next token
            parser.nextToken();
        }

        if (value == null) {
            throw new QueryParsingException(index, "No value specified for fuzzy query");
        }

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                query = smartNameFieldMappers.mapper().fuzzyQuery(value, minSimilarity, prefixLength, maxExpansions);
            }
        }
        if (query == null) {
            query = new FuzzyQuery(new Term(fieldName, value), Float.parseFloat(minSimilarity), prefixLength, maxExpansions);
        }
        query.setBoost(boost);

        return wrapSmartNameQuery(query, smartNameFieldMappers, parseContext);
    }
}
