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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.OldCompletionFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.suggest.SuggestUtils.parseSuggestContext;

/**
 * Parses query options for {@link CompletionSuggester}
 *
 * Acceptable input:
 * {
 *     "field" : STRING
 *     "size" : INT
 *     "fuzzy" : BOOLEAN | FUZZY_OBJECT
 *     "contexts" : QUERY_CONTEXTS
 *     "regex" : REGEX_OBJECT
 * }
 *
 * see {@link ContextMappings#parseQueryContext(XContentParser)} for QUERY_CONTEXTS
 *
 * FUZZY_OBJECT : {
 *     "edit_distance" : STRING | INT
 *     "transpositions" : BOOLEAN
 *     "min_length" : INT
 *     "prefix_length" : INT
 *     "unicode_aware" : BOOLEAN
 *     "max_determinized_states" : INT
 * }
 *
 * REGEX_OBJECT: {
 *     "flags" : REGEX_FLAGS
 *     "max_determinized_states" : INT
 * }
 *
 * see {@link RegexpFlag} for REGEX_FLAGS
 */
public class CompletionSuggestParser implements SuggestContextParser {

    private CompletionSuggester completionSuggester;
    private static final ParseField FUZZINESS = Fuzziness.FIELD.withDeprecation("edit_distance");
    private org.elasticsearch.search.suggest.completion.old.CompletionSuggester oldCompletionSuggester;

    public CompletionSuggestParser(CompletionSuggester completionSuggester) {
        this.completionSuggester = completionSuggester;
    }

    @Override
    public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, MapperService mapperService,
                                                           IndexQueryParserService queryParserService) throws IOException {
        XContentParser.Token token;
        String fieldName = null;
        CompletionSuggestionContext suggestion = new CompletionSuggestionContext(completionSuggester);
        
        XContentParser contextParser = null;
        CompletionSuggestionBuilder.FuzzyOptionsBuilder fuzzyOptions = null;
        CompletionSuggestionBuilder.RegexOptionsBuilder regexOptions = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (!parseSuggestContext(parser, mapperService, fieldName, suggestion, queryParserService.parseFieldMatcher())) {
                    if (token == XContentParser.Token.VALUE_BOOLEAN && "fuzzy".equals(fieldName)) {
                        if (parser.booleanValue()) {
                            fuzzyOptions = new CompletionSuggestionBuilder.FuzzyOptionsBuilder();
                        }
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("fuzzy".equals(fieldName)) {
                    fuzzyOptions = new CompletionSuggestionBuilder.FuzzyOptionsBuilder();
                    String fuzzyConfigName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fuzzyConfigName = parser.currentName();
                        } else if (token.isValue()) {
                            if (queryParserService.parseFieldMatcher().match(fuzzyConfigName, FUZZINESS)) {
                                fuzzyOptions.setFuzziness(Fuzziness.parse(parser).asDistance());
                            } else if ("transpositions".equals(fuzzyConfigName)) {
                                fuzzyOptions.setTranspositions(parser.booleanValue());
                            } else if ("min_length".equals(fuzzyConfigName) || "minLength".equals(fuzzyConfigName)) {
                                fuzzyOptions.setFuzzyMinLength(parser.intValue());
                            } else if ("prefix_length".equals(fuzzyConfigName) || "prefixLength".equals(fuzzyConfigName)) {
                                fuzzyOptions.setFuzzyPrefixLength(parser.intValue());
                            } else if ("unicode_aware".equals(fuzzyConfigName) || "unicodeAware".equals(fuzzyConfigName)) {
                                fuzzyOptions.setUnicodeAware(parser.booleanValue());
                            } else if ("max_determinized_states".equals(fuzzyConfigName)) {
                                fuzzyOptions.setMaxDeterminizedStates(parser.intValue());
                            } else {
                                throw new IllegalArgumentException("[fuzzy] query does not support [" + fuzzyConfigName + "]");
                            }
                        }
                    }
                } else if ("contexts".equals(fieldName) || "context".equals(fieldName)) {
                    // Copy the current structure. We will parse, once the mapping is provided
                    XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                    builder.copyCurrentStructure(parser);
                    BytesReference bytes = builder.bytes();
                    contextParser = XContentFactory.xContent(bytes).createParser(bytes);
                } else if ("regex".equals(fieldName)) {
                    regexOptions = new CompletionSuggestionBuilder.RegexOptionsBuilder();
                    String currentFieldName = fieldName;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            if ("flags".equals(currentFieldName)) {
                                String flags = parser.textOrNull();
                                regexOptions.setFlags(flags);
                            } else if ("max_determinized_states".equals(currentFieldName)) {
                                regexOptions.setMaxDeterminizedStates(parser.intValue());
                            } else {
                                throw new IllegalArgumentException("[regexp] query does not support [" + currentFieldName + "]");
                            }
                        }
                    }
                } else {
                    throw new IllegalArgumentException("suggester [completion] doesn't support field [" + fieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("suggester [completion] doesn't support field [" + fieldName + "]");
            }
        }
        MappedFieldType mappedFieldType = mapperService.smartNameFieldType(suggestion.getField());
        if (mappedFieldType == null) {
            throw new ElasticsearchException("Field [" + suggestion.getField() + "] is not a completion suggest field");
        } else if (mappedFieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            CompletionFieldMapper.CompletionFieldType type = (CompletionFieldMapper.CompletionFieldType) mappedFieldType;
            if (type.hasContextMappings() == false && contextParser != null) {
                throw new IllegalArgumentException("suggester [" + type.names().fullName() + "] doesn't expect any context");
            }
            Map<String, ContextMapping.QueryContexts> queryContexts = Collections.emptyMap();
            if (type.hasContextMappings() && contextParser != null) {
                contextParser.nextToken();
                queryContexts = type.getContextMappings().parseQueryContext(contextParser);
                contextParser.close();
            }

            suggestion.fieldType(type);
            suggestion.setFuzzyOptionsBuilder(fuzzyOptions);
            suggestion.setRegexOptionsBuilder(regexOptions);
            suggestion.setQueryContexts(queryContexts);
            // TODO: pass a query builder or the query itself?
            // now we do it in CompletionSuggester#toQuery(CompletionSuggestionContext)
            return suggestion;

        } else if (mappedFieldType instanceof OldCompletionFieldMapper.CompletionFieldType) {
            org.elasticsearch.search.suggest.completion.old.CompletionSuggestionContext oldSuggestionContext =
                    new org.elasticsearch.search.suggest.completion.old.CompletionSuggestionContext(oldCompletionSuggester);
            OldCompletionFieldMapper.CompletionFieldType fieldType = (OldCompletionFieldMapper.CompletionFieldType) mappedFieldType;
            oldSuggestionContext.setAnalyzer(suggestion.getAnalyzer());
            oldSuggestionContext.setField(suggestion.getField());
            oldSuggestionContext.setSize(suggestion.getSize());
            if (suggestion.getShardSize() > 0) {
                oldSuggestionContext.setShardSize(suggestion.getShardSize());
            }

            if (fuzzyOptions != null) {
                oldSuggestionContext.setFuzzy(true);
                oldSuggestionContext.setFuzzyEditDistance(fuzzyOptions.getEditDistance());
                oldSuggestionContext.setFuzzyMinLength(fuzzyOptions.getFuzzyMinLength());
                oldSuggestionContext.setFuzzyPrefixLength(fuzzyOptions.getFuzzyPrefixLength());
                oldSuggestionContext.setFuzzyTranspositions(fuzzyOptions.isTranspositions());
                oldSuggestionContext.setFuzzyUnicodeAware(fuzzyOptions.isUnicodeAware());
            }

            oldSuggestionContext.fieldType(fieldType);
            if (fieldType.requiresContext()) {
                if (contextParser == null) {
                    throw new IllegalArgumentException("suggester [completion] requires context to be setup");
                } else {
                    contextParser.nextToken();
                    List<org.elasticsearch.search.suggest.completion.old.context.ContextMapping.ContextQuery> contextQueries =
                            org.elasticsearch.search.suggest.completion.old.context.ContextMapping.ContextQuery.parseQueries(fieldType.getContextMapping(), contextParser);
                    contextParser.close();
                    oldSuggestionContext.setContextQuery(contextQueries);
                }
            } else if (contextParser != null) {
                throw new IllegalArgumentException("suggester [completion] doesn't expect any context");
            }
            return oldSuggestionContext;
        } else {
            throw new ElasticsearchException("Field [" + suggestion.getField() + "] is not a completion suggest field");
        }
    }

    public void setOldCompletionSuggester(org.elasticsearch.search.suggest.completion.old.CompletionSuggester oldCompletionSuggester) {
        this.oldCompletionSuggester = oldCompletionSuggester;
    }
}
