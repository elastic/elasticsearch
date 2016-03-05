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

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestUtils.Fields;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;

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

    private static ObjectParser<CompletionSuggestionContext, ContextAndSuggest> TLP_PARSER = new ObjectParser<>(CompletionSuggestionBuilder.SUGGESTION_NAME, null);
    static {
        TLP_PARSER.declareStringArray(CompletionSuggestionContext::setPayloadFields, CompletionSuggestionBuilder.PAYLOAD_FIELD);
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) -> {
                if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                    if (parser.booleanValue()) {
                        completionSuggestionContext.setFuzzyOptions(new FuzzyOptions.Builder().build());
                    }
                } else {
                    completionSuggestionContext.setFuzzyOptions(FuzzyOptions.parse(parser));
                }
            },
            FuzzyOptions.FUZZY_OPTIONS, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) -> completionSuggestionContext.setRegexOptions(RegexOptions.parse(parser)),
            RegexOptions.REGEX_OPTIONS, ObjectParser.ValueType.OBJECT);
        TLP_PARSER.declareString(SuggestionSearchContext.SuggestionContext::setField, Fields.FIELD);
        TLP_PARSER.declareField((p, v, c) -> {
            String analyzerName = p.text();
            Analyzer analyzer = c.mapperService.analysisService().analyzer(analyzerName);
            if (analyzer == null) {
                throw new IllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
            }
            v.setAnalyzer(analyzer);
        }, Fields.ANALYZER, ObjectParser.ValueType.STRING);
        TLP_PARSER.declareInt(SuggestionSearchContext.SuggestionContext::setSize, Fields.SIZE);
        TLP_PARSER.declareInt(SuggestionSearchContext.SuggestionContext::setShardSize, Fields.SHARD_SIZE);
        TLP_PARSER.declareField((p, v, c) -> {
            // Copy the current structure. We will parse, once the mapping is provided
            XContentBuilder builder = XContentFactory.contentBuilder(p.contentType());
            builder.copyCurrentStructure(p);
            BytesReference bytes = builder.bytes();
            c.contextParser = XContentFactory.xContent(bytes).createParser(bytes);
            p.skipChildren();
        }, CompletionSuggestionBuilder.CONTEXTS_FIELD, ObjectParser.ValueType.OBJECT); // context is deprecated
    }

    private static class ContextAndSuggest {
        XContentParser contextParser;
        final MapperService mapperService;

        ContextAndSuggest(MapperService mapperService) {
            this.mapperService = mapperService;
        }
    }

    private final CompletionSuggester completionSuggester;

    public CompletionSuggestParser(CompletionSuggester completionSuggester) {
        this.completionSuggester = completionSuggester;
    }

    @Override
    public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, QueryShardContext shardContext) throws IOException {
        MapperService mapperService = shardContext.getMapperService();
        final CompletionSuggestionContext suggestion = new CompletionSuggestionContext(shardContext);
        final ContextAndSuggest contextAndSuggest = new ContextAndSuggest(mapperService);
        TLP_PARSER.parse(parser, suggestion, contextAndSuggest);
        final XContentParser contextParser = contextAndSuggest.contextParser;
        MappedFieldType mappedFieldType = mapperService.fullName(suggestion.getField());
        if (mappedFieldType == null) {
            throw new ElasticsearchException("Field [" + suggestion.getField() + "] is not a completion suggest field");
        } else if (mappedFieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            CompletionFieldMapper.CompletionFieldType type = (CompletionFieldMapper.CompletionFieldType) mappedFieldType;
            if (type.hasContextMappings() == false && contextParser != null) {
                throw new IllegalArgumentException("suggester [" + type.name() + "] doesn't expect any context");
            }
            suggestion.setQueryContexts(CompletionSuggestionBuilder.parseQueryContexts(contextParser, type));
            suggestion.setFieldType(type);
            return suggestion;
        } else {
            throw new IllegalArgumentException("Field [" + suggestion.getField() + "] is not a completion suggest field");
        }
    }

}
