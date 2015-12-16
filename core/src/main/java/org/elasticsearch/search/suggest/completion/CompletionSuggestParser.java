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
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static ObjectParser<CompletionSuggestionContext, ContextAndSuggest> TLP_PARSER = new ObjectParser<>("completion", null);
    private static ObjectParser<CompletionSuggestionBuilder.RegexOptionsBuilder, ContextAndSuggest> REGEXP_PARSER = new ObjectParser<>("regexp", CompletionSuggestionBuilder.RegexOptionsBuilder::new);
    private static ObjectParser<CompletionSuggestionBuilder.FuzzyOptionsBuilder, ContextAndSuggest> FUZZY_PARSER = new ObjectParser<>("fuzzy", CompletionSuggestionBuilder.FuzzyOptionsBuilder::new);
    static {
        FUZZY_PARSER.declareInt(CompletionSuggestionBuilder.FuzzyOptionsBuilder::setFuzzyMinLength, new ParseField("min_length"));
        FUZZY_PARSER.declareInt(CompletionSuggestionBuilder.FuzzyOptionsBuilder::setMaxDeterminizedStates, new ParseField("max_determinized_states"));
        FUZZY_PARSER.declareBoolean(CompletionSuggestionBuilder.FuzzyOptionsBuilder::setUnicodeAware, new ParseField("unicode_aware"));
        FUZZY_PARSER.declareInt(CompletionSuggestionBuilder.FuzzyOptionsBuilder::setFuzzyPrefixLength, new ParseField("prefix_length"));
        FUZZY_PARSER.declareBoolean(CompletionSuggestionBuilder.FuzzyOptionsBuilder::setTranspositions, new ParseField("transpositions"));
        FUZZY_PARSER.declareValue((a, b) -> {
            try {
                a.setFuzziness(Fuzziness.parse(b).asDistance());
            } catch (IOException e) {
                throw new ElasticsearchException(e);
            }
        }, new ParseField("fuzziness"));
        REGEXP_PARSER.declareInt(CompletionSuggestionBuilder.RegexOptionsBuilder::setMaxDeterminizedStates, new ParseField("max_determinized_states"));
        REGEXP_PARSER.declareStringOrNull(CompletionSuggestionBuilder.RegexOptionsBuilder::setFlags, new ParseField("flags"));

        TLP_PARSER.declareStringArray(CompletionSuggestionContext::setPayloadFields, new ParseField("payload"));
        TLP_PARSER.declareObjectOrDefault(CompletionSuggestionContext::setFuzzyOptionsBuilder, FUZZY_PARSER, CompletionSuggestionBuilder.FuzzyOptionsBuilder::new, new ParseField("fuzzy"));
        TLP_PARSER.declareObject(CompletionSuggestionContext::setRegexOptionsBuilder, REGEXP_PARSER, new ParseField("regexp"));
        TLP_PARSER.declareString(SuggestionSearchContext.SuggestionContext::setField, new ParseField("field"));
        TLP_PARSER.declareField((p, v, c) -> {
            String analyzerName = p.text();
            Analyzer analyzer = c.mapperService.analysisService().analyzer(analyzerName);
            if (analyzer == null) {
                throw new IllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
            }
            v.setAnalyzer(analyzer);
        }, new ParseField("analyzer"), ObjectParser.ValueType.STRING);
        TLP_PARSER.declareString(SuggestionSearchContext.SuggestionContext::setField, new ParseField("analyzer"));
        TLP_PARSER.declareInt(SuggestionSearchContext.SuggestionContext::setSize, new ParseField("size"));
        TLP_PARSER.declareInt(SuggestionSearchContext.SuggestionContext::setShardSize, new ParseField("size"));
        TLP_PARSER.declareField((p, v, c) -> {
            // Copy the current structure. We will parse, once the mapping is provided
            XContentBuilder builder = XContentFactory.contentBuilder(p.contentType());
            builder.copyCurrentStructure(p);
            BytesReference bytes = builder.bytes();
            c.contextParser = XContentFactory.xContent(bytes).createParser(bytes);
            p.skipChildren();
        }, new ParseField("contexts", "context"), ObjectParser.ValueType.OBJECT); // context is deprecated
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
    public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, MapperService mapperService, IndexFieldDataService fieldDataService,
                                                           HasContextAndHeaders headersContext) throws IOException {
        final CompletionSuggestionContext suggestion = new CompletionSuggestionContext(completionSuggester, mapperService, fieldDataService);
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
            Map<String, List<ContextMapping.QueryContext>> queryContexts = Collections.emptyMap();
            if (type.hasContextMappings() && contextParser != null) {
                ContextMappings contextMappings = type.getContextMappings();
                contextParser.nextToken();
                queryContexts = new HashMap<>(contextMappings.size());
                assert contextParser.currentToken() == XContentParser.Token.START_OBJECT;
                XContentParser.Token currentToken;
                String currentFieldName;
                while ((currentToken = contextParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (currentToken == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = contextParser.currentName();
                        final ContextMapping mapping = contextMappings.get(currentFieldName);
                        queryContexts.put(currentFieldName, mapping.parseQueryContext(contextParser));
                    }
                }
                contextParser.close();
            }
            suggestion.setFieldType(type);
            suggestion.setQueryContexts(queryContexts);
            return suggestion;
        } else {
            throw new IllegalArgumentException("Field [" + suggestion.getField() + "] is not a completion suggest field");
        }
    }



}
