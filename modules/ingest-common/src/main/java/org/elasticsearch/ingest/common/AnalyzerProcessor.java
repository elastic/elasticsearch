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

package org.elasticsearch.ingest.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction.getAnonymousSettings;
import static org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction.getNaIndexSettings;

/**
 * Processor that splits fields content into different items based on the occurrence of a specified separator.
 * New field value will be an array containing all of the different extracted items.
 * Throws exception if the field is null or a type other than string.
 */
public final class AnalyzerProcessor extends AbstractProcessor {

    public static final String TYPE = "analyzer";

    private final String targetField;

    private final String field;

    private final Analyzer analyzer;

    private final boolean customAnalyzer;

    AnalyzerProcessor(String tag, String field, String targetField, Analyzer analyzer, boolean customAnalyzer) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.analyzer = analyzer;
        this.customAnalyzer = customAnalyzer;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    Analyzer getAnalyzer() {
        return analyzer;
    }

    @Override
    public void execute(IngestDocument document) {
        Object oldVal = document.getFieldValue(field, Object.class);
        if (oldVal == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot be analyzed.");
        }
        List<String> splitList = new ArrayList<>();
        if (oldVal instanceof String) {
            split(splitList, (String) oldVal);
        } else if (oldVal instanceof ArrayList) {
            for (Object obj : (ArrayList) oldVal) {
                split(splitList, obj.toString());
            }
        } else {
            throw new IllegalArgumentException("field [" + field + "] has type [" + oldVal.getClass().getName() +
                "] and cannot be analyzed");
        }
        document.setFieldValue(targetField, splitList);
    }

    private void split(List<String> splitList, String val) {
        try (TokenStream stream = analyzer.tokenStream(field, val)) {
            stream.reset();
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            while (stream.incrementToken()) {
                splitList.add(term.toString());
            }
            stream.end();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to analyze field [" + field + "]", e);
        }
    }

    @Override
    public void close() {
        if (customAnalyzer) {
            analyzer.close();
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {

        private final AnalysisRegistry analysisRegistry;
        private final Environment environment;

        public Factory(Environment environment, AnalysisRegistry analysisRegistry) {
            this.environment = environment;
            this.analysisRegistry = analysisRegistry;
        }

        @Override
        public AnalyzerProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                        Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);

            final Analyzer analyzer;
            boolean closeAnalyzer = false;
            if (config.containsKey("analyzer")) {
                if (config.containsKey("filter") || config.containsKey("token_filter") ||
                    config.containsKey("char_filter") || config.containsKey("tokenizer")) {
                    throw new ElasticsearchParseException("custom tokenizer or filters cannot be specified if a named analyzer is used");
                }
                String analyzerName = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "analyzer");
                analyzer = analysisRegistry.getAnalyzer(analyzerName);
                if (analyzer == null) {
                    throw new IllegalArgumentException("Unknown analyzer [" + analyzerName + "]");
                }
            } else if (config.containsKey("tokenizer")) {
                Object tokenizer = ConfigurationUtils.readObject(TYPE, processorTag, config, "tokenizer");
                TokenizerFactory tokenizerFactory = parseDefinition(tokenizer, analysisRegistry::getTokenizerProvider, "tokenizer", -1);

                List<Object> filters = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "filter");
                if (filters == null) {
                    filters = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "token_filter");
                }
                TokenFilterFactory[] tokenFilterFactories = parseTokenFilterFactories(filters);

                List<Object> charFilters = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "char_filter");
                CharFilterFactory[] charFilterFactories = parseCharFilterFactories(charFilters);

                analyzer = new CustomAnalyzer(tokenizerFactory, charFilterFactories, tokenFilterFactories);
                closeAnalyzer = true;
            } else {
                throw new ElasticsearchParseException("missing analyzer definition");
            }
            return new AnalyzerProcessor(processorTag, field, targetField, analyzer, closeAnalyzer);
        }

        TokenFilterFactory[] parseTokenFilterFactories(List<Object> filterDefinitions) throws IOException {
            if (filterDefinitions != null && filterDefinitions.size() > 0) {
                TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[filterDefinitions.size()];
                for (int i = 0; i < filterDefinitions.size(); i++) {
                    tokenFilterFactories[i] = parseDefinition(filterDefinitions.get(i), analysisRegistry::getTokenFilterProvider,
                        "token_filter", i);
                }
                return tokenFilterFactories;
            } else {
                return new TokenFilterFactory[0];
            }
        }

        CharFilterFactory[] parseCharFilterFactories(List<Object> filterDefinitions) throws IOException {
            if (filterDefinitions != null && filterDefinitions.size() > 0) {
                CharFilterFactory[] charFilterFactories = new CharFilterFactory[filterDefinitions.size()];
                for (int i = 0; i < filterDefinitions.size(); i++) {
                    charFilterFactories[i] = parseDefinition(filterDefinitions.get(i), analysisRegistry::getCharFilterProvider,
                        "char_filter", i);
                }
                return charFilterFactories;
            } else {
                return new CharFilterFactory[0];
            }
        }

        private <T> T parseDefinition(Object nameOrDefinition, Function<String, AnalysisModule.AnalysisProvider<T>> factory,
                                      String type, int i) throws IOException {
            AnalysisModule.AnalysisProvider<T> filterFactoryProvider;
            if (nameOrDefinition instanceof String) {
                String charFilterName = (String) nameOrDefinition;
                filterFactoryProvider = factory.apply(charFilterName);
                if (filterFactoryProvider == null) {
                    throw new IllegalArgumentException("failed to find global " + type + " [" + charFilterName + "]");
                }
                return filterFactoryProvider.get(environment, charFilterName);
            } else if (nameOrDefinition instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> map = (Map<String, Object>) nameOrDefinition;
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(map);
                Settings settings = Settings.builder().loadFromSource(builder.string()).build();
                settings = getAnonymousSettings(settings);
                String filterTypeName = settings.get("type");
                if (filterTypeName == null) {
                    throw new IllegalArgumentException("Missing [type] setting for anonymous " + type + ": " + map);
                }
                AnalysisModule.AnalysisProvider<T> charFilterFactoryFactory = factory.apply(filterTypeName);
                if (charFilterFactoryFactory == null) {
                    throw new IllegalArgumentException("failed to find global " + type + " [" + filterTypeName + "]");
                }
                // Need to set anonymous "name" of analysis component
                String anonymousName = "_anonymous_" + type + (i < 0 ? "_[" + i + "]" : "_");
                return charFilterFactoryFactory.get(getNaIndexSettings(settings), environment, anonymousName, settings);
            } else {
                throw new IllegalArgumentException("failed to find or create " + type + " for [" + nameOrDefinition + "]");
            }
        }

    }
}
