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
package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.context.ContextMapping.ContextQuery;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.suggest.SuggestUtils.parseSuggestContext;

/**
 *
 */
public class CompletionSuggestParser implements SuggestContextParser {

    private CompletionSuggester completionSuggester;
    private static final ParseField FUZZINESS = Fuzziness.FIELD.withDeprecation("edit_distance");

    public CompletionSuggestParser(CompletionSuggester completionSuggester) {
        this.completionSuggester = completionSuggester;
    }

    @Override
    public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, MapperService mapperService,
            IndexQueryParserService queryParserService, HasContextAndHeaders headersContext) throws IOException {
        XContentParser.Token token;
        String fieldName = null;
        CompletionSuggestionContext suggestion = new CompletionSuggestionContext(completionSuggester);

        XContentParser contextParser = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (!parseSuggestContext(parser, mapperService, fieldName, suggestion, queryParserService.parseFieldMatcher()))  {
                    if (token == XContentParser.Token.VALUE_BOOLEAN && "fuzzy".equals(fieldName)) {
                        suggestion.setFuzzy(parser.booleanValue());
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if("fuzzy".equals(fieldName)) {
                    suggestion.setFuzzy(true);
                    String fuzzyConfigName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fuzzyConfigName = parser.currentName();
                        } else if (token.isValue()) {
                            if (queryParserService.parseFieldMatcher().match(fuzzyConfigName, FUZZINESS)) {
                                suggestion.setFuzzyEditDistance(Fuzziness.parse(parser).asDistance());
                            } else if ("transpositions".equals(fuzzyConfigName)) {
                                suggestion.setFuzzyTranspositions(parser.booleanValue());
                            } else if ("min_length".equals(fuzzyConfigName) || "minLength".equals(fuzzyConfigName)) {
                                suggestion.setFuzzyMinLength(parser.intValue());
                            } else if ("prefix_length".equals(fuzzyConfigName) || "prefixLength".equals(fuzzyConfigName)) {
                                suggestion.setFuzzyPrefixLength(parser.intValue());
                            } else if ("unicode_aware".equals(fuzzyConfigName) || "unicodeAware".equals(fuzzyConfigName)) {
                                suggestion.setFuzzyUnicodeAware(parser.booleanValue());
                            }
                        }
                    }
                } else if("context".equals(fieldName)) {
                    // Copy the current structure. We will parse, once the mapping is provided
                    XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                    builder.copyCurrentStructure(parser);
                    BytesReference bytes = builder.bytes();
                    contextParser = parser.contentType().xContent().createParser(bytes);
                } else {
                    throw new IllegalArgumentException("suggester [completion] doesn't support field [" + fieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("suggester[completion]  doesn't support field [" + fieldName + "]");
            }
        }

        suggestion.fieldType((CompletionFieldMapper.CompletionFieldType) mapperService.smartNameFieldType(suggestion.getField()));

        CompletionFieldMapper.CompletionFieldType fieldType = suggestion.fieldType();
        if (fieldType != null) {
            if (fieldType.requiresContext()) {
                if (contextParser == null) {
                    throw new IllegalArgumentException("suggester [completion] requires context to be setup");
                } else {
                    contextParser.nextToken();
                    List<ContextQuery> contextQueries = ContextQuery.parseQueries(fieldType.getContextMapping(), contextParser);
                    suggestion.setContextQuery(contextQueries);
                }
            } else if (contextParser != null) {
                throw new IllegalArgumentException("suggester [completion] doesn't expect any context");
            }
        }
        return suggestion;
    }

}
