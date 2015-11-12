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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class FuzzyQueryParser implements QueryParser<FuzzyQueryBuilder> {

    @Override
    public String[] names() {
        return new String[]{ FuzzyQueryBuilder.NAME };
    }

    @Override
    public FuzzyQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[fuzzy] query malformed, no field");
        }

        String fieldName = parser.currentName();
        Object value = null;

        Fuzziness fuzziness = FuzzyQueryBuilder.DEFAULT_FUZZINESS;
        int prefixLength = FuzzyQueryBuilder.DEFAULT_PREFIX_LENGTH;
        int maxExpansions = FuzzyQueryBuilder.DEFAULT_MAX_EXPANSIONS;
        boolean transpositions = FuzzyQueryBuilder.DEFAULT_TRANSPOSITIONS;
        String rewrite = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if ("term".equals(currentFieldName)) {
                        value = parser.objectBytes();
                    } else if ("value".equals(currentFieldName)) {
                        value = parser.objectBytes();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, Fuzziness.FIELD)) {
                        fuzziness = Fuzziness.parse(parser);
                    } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                        prefixLength = parser.intValue();
                    } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                        maxExpansions = parser.intValue();
                    } else if ("transpositions".equals(currentFieldName)) {
                        transpositions = parser.booleanValue();
                    } else if ("rewrite".equals(currentFieldName)) {
                        rewrite = parser.textOrNull();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[fuzzy] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.objectBytes();
            // move to the next token
            parser.nextToken();
        }

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "no value specified for fuzzy query");
        }
        return new FuzzyQueryBuilder(fieldName, value)
                .fuzziness(fuzziness)
                .prefixLength(prefixLength)
                .maxExpansions(maxExpansions)
                .transpositions(transpositions)
                .rewrite(rewrite)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public FuzzyQueryBuilder getBuilderPrototype() {
        return FuzzyQueryBuilder.PROTOTYPE;
    }
}
