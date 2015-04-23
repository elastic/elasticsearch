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
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionContext.DirectCandidateGenerator;

import java.io.IOException;

public final class PhraseSuggestParser implements SuggestContextParser {

    private PhraseSuggester suggester;

    public PhraseSuggestParser(PhraseSuggester suggester) {
        this.suggester = suggester;
    }

    @Override
    public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, MapperService mapperService) throws IOException {
        PhraseSuggestionContext suggestion = new PhraseSuggestionContext(suggester);
        XContentParser.Token token;
        String fieldName = null;
        boolean gramSizeSet = false; 
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (!SuggestUtils.parseSuggestContext(parser, mapperService, fieldName, suggestion)) {
                    if ("real_word_error_likelihood".equals(fieldName) || "realWorldErrorLikelihood".equals(fieldName)) {
                        suggestion.setRealWordErrorLikelihood(parser.floatValue());
                        if (suggestion.realworldErrorLikelyhood() <= 0.0) {
                            throw new ElasticsearchIllegalArgumentException("real_word_error_likelihood must be > 0.0");
                        }
                    } else if ("confidence".equals(fieldName)) {
                        suggestion.setConfidence(parser.floatValue());
                        if (suggestion.confidence() < 0.0) {
                            throw new ElasticsearchIllegalArgumentException("confidence must be >= 0.0");
                        }
                    } else if ("separator".equals(fieldName)) {
                        suggestion.setSeparator(new BytesRef(parser.text()));
                    } else if ("max_errors".equals(fieldName) || "maxErrors".equals(fieldName)) {
                        suggestion.setMaxErrors(parser.floatValue());
                        if (suggestion.maxErrors() <= 0.0) {
                            throw new ElasticsearchIllegalArgumentException("max_error must be > 0.0");
                        }
                    } else if ("gram_size".equals(fieldName) || "gramSize".equals(fieldName)) {
                        suggestion.setGramSize(parser.intValue());
                        if (suggestion.gramSize() < 1) {
                            throw new ElasticsearchIllegalArgumentException("gram_size must be >= 1");
                        }
                        gramSizeSet = true;
                    } else if ("force_unigrams".equals(fieldName) || "forceUnigrams".equals(fieldName)) {
                        suggestion.setRequireUnigram(parser.booleanValue());
                    } else if ("token_limit".equals(fieldName) || "tokenLimit".equals(fieldName)) {
                        int tokenLimit = parser.intValue();
                        if (tokenLimit <= 0) {
                            throw new ElasticsearchIllegalArgumentException("token_limit must be >= 1");
                        }
                        suggestion.setTokenLimit(tokenLimit);
                    } else {
                        throw new ElasticsearchIllegalArgumentException("suggester[phrase] doesn't support field [" + fieldName + "]");
                    }
                }
            } else if (token == Token.START_ARRAY) {
                if ("direct_generator".equals(fieldName) || "directGenerator".equals(fieldName)) {
                    // for now we only have a single type of generators
                    while ((token = parser.nextToken()) == Token.START_OBJECT) {
                        PhraseSuggestionContext.DirectCandidateGenerator generator = new PhraseSuggestionContext.DirectCandidateGenerator();
                        while ((token = parser.nextToken()) != Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                fieldName = parser.currentName();
                            }
                            if (token.isValue()) {
                                parseCandidateGenerator(parser, mapperService, fieldName, generator);
                            }
                        }
                        verifyGenerator(generator);
                        suggestion.addGenerator(generator);
                    }
                } else {
                    throw new ElasticsearchIllegalArgumentException("suggester[phrase]  doesn't support array field [" + fieldName + "]");
                }
            } else if (token == Token.START_OBJECT) {
                if ("smoothing".equals(fieldName)) {
                    parseSmoothingModel(parser, suggestion, fieldName);
                } else if ("highlight".equals(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("pre_tag".equals(fieldName) || "preTag".equals(fieldName)) {
                                suggestion.setPreTag(parser.utf8Bytes());
                            } else if ("post_tag".equals(fieldName) || "postTag".equals(fieldName)) {
                                suggestion.setPostTag(parser.utf8Bytes());
                            } else {
                                throw new ElasticsearchIllegalArgumentException(
                                    "suggester[phrase][highlight] doesn't support field [" + fieldName + "]");
                            }
                        }
                    }
                } else if ("collate".equals(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if ("query".equals(fieldName) || "filter".equals(fieldName)) {
                            String templateNameOrTemplateContent;
                            if (token == XContentParser.Token.START_OBJECT) {
                                XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
                                builder.copyCurrentStructure(parser);
                                templateNameOrTemplateContent = builder.string();
                            } else {
                                templateNameOrTemplateContent = parser.text();
                            }
                            if (templateNameOrTemplateContent == null) {
                                throw new ElasticsearchIllegalArgumentException("suggester[phrase][collate] no query/filter found in collate object");
                            }
                            if (suggestion.getCollateFilterScript() != null) {
                                throw new ElasticsearchIllegalArgumentException("suggester[phrase][collate] filter already set, doesn't support additional [" + fieldName + "]");
                            }
                            if (suggestion.getCollateQueryScript() != null) {
                                throw new ElasticsearchIllegalArgumentException("suggester[phrase][collate] query already set, doesn't support additional [" + fieldName + "]");
                            }
                            CompiledScript compiledScript = suggester.scriptService().compile(new Script(MustacheScriptEngineService.NAME, templateNameOrTemplateContent, ScriptType.INLINE, null), ScriptContext.Standard.SEARCH);
                            if ("query".equals(fieldName)) {
                                suggestion.setCollateQueryScript(compiledScript);
                            } else {
                                suggestion.setCollateFilterScript(compiledScript);
                            }
                        } else if ("preference".equals(fieldName)) {
                            suggestion.setPreference(parser.text());
                        } else if ("params".equals(fieldName)) {
                            suggestion.setCollateScriptParams(parser.map());
                        } else if ("prune".equals(fieldName)) {
                            if (parser.isBooleanValue()) {
                                suggestion.setCollatePrune(parser.booleanValue());
                            } else {
                                throw new ElasticsearchIllegalArgumentException("suggester[phrase][collate] prune must be either 'true' or 'false'");
                            }
                        } else {
                            throw new ElasticsearchIllegalArgumentException(
                                    "suggester[phrase][collate] doesn't support field [" + fieldName + "]");
                        }
                    }
                } else {
                    throw new ElasticsearchIllegalArgumentException("suggester[phrase]  doesn't support array field [" + fieldName + "]");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("suggester[phrase] doesn't support field [" + fieldName + "]");
            }
        }
        
        if (suggestion.getField() == null) {
            throw new ElasticsearchIllegalArgumentException("The required field option is missing");
        }

        FieldMapper fieldMapper = mapperService.smartNameFieldMapper(suggestion.getField());
        if (fieldMapper == null) {
            throw new ElasticsearchIllegalArgumentException("No mapping found for field [" + suggestion.getField() + "]");
        } else if (suggestion.getAnalyzer() == null) {
            // no analyzer name passed in, so try the field's analyzer, or the default analyzer
            if (fieldMapper.searchAnalyzer() == null) {
                suggestion.setAnalyzer(mapperService.searchAnalyzer());
            } else {
                suggestion.setAnalyzer(fieldMapper.searchAnalyzer());
            }
        }
        
        if (suggestion.model() == null) {
            suggestion.setModel(StupidBackoffScorer.FACTORY);
        }
        
        if (!gramSizeSet || suggestion.generators().isEmpty()) {
            final ShingleTokenFilterFactory.Factory shingleFilterFactory = SuggestUtils.getShingleFilterFactory(suggestion.getAnalyzer());
            if (!gramSizeSet) {
                // try to detect the shingle size
                if (shingleFilterFactory != null) {
                    suggestion.setGramSize(shingleFilterFactory.getMaxShingleSize());
                    if (suggestion.getAnalyzer() == null && shingleFilterFactory.getMinShingleSize() > 1 && !shingleFilterFactory.getOutputUnigrams()) {
                        throw new ElasticsearchIllegalArgumentException("The default analyzer for field: [" + suggestion.getField() + "] doesn't emit unigrams. If this is intentional try to set the analyzer explicitly");
                    }
                }
            }
            if (suggestion.generators().isEmpty()) {
                if (shingleFilterFactory != null && shingleFilterFactory.getMinShingleSize() > 1 && !shingleFilterFactory.getOutputUnigrams() && suggestion.getRequireUnigram()) {
                    throw new ElasticsearchIllegalArgumentException("The default candidate generator for phrase suggest can't operate on field: [" + suggestion.getField() + "] since it doesn't emit unigrams. If this is intentional try to set the candidate generator field explicitly");
                }
                // use a default generator on the same field
                DirectCandidateGenerator generator = new DirectCandidateGenerator();
                generator.setField(suggestion.getField());
                suggestion.addGenerator(generator);
            }
        }
        
        
        
        return suggestion;
    }

    public void parseSmoothingModel(XContentParser parser, PhraseSuggestionContext suggestion, String fieldName) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                if ("linear".equals(fieldName)) {
                    ensureNoSmoothing(suggestion);
                    final double[] lambdas = new double[3];
                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        }
                        if (token.isValue()) {
                            if ("trigram_lambda".equals(fieldName) || "trigramLambda".equals(fieldName)) {
                                lambdas[0] = parser.doubleValue();
                                if (lambdas[0] < 0) {
                                    throw new ElasticsearchIllegalArgumentException("trigram_lambda must be positive");
                                }
                            } else if ("bigram_lambda".equals(fieldName) || "bigramLambda".equals(fieldName)) {
                                lambdas[1] = parser.doubleValue();
                                if (lambdas[1] < 0) {
                                    throw new ElasticsearchIllegalArgumentException("bigram_lambda must be positive");
                                }
                            } else if ("unigram_lambda".equals(fieldName) || "unigramLambda".equals(fieldName)) {
                                lambdas[2] = parser.doubleValue();
                                if (lambdas[2] < 0) {
                                    throw new ElasticsearchIllegalArgumentException("unigram_lambda must be positive");
                                }
                            } else {
                                throw new ElasticsearchIllegalArgumentException(
                                        "suggester[phrase][smoothing][linear] doesn't support field [" + fieldName + "]");
                            }
                        }
                    }
                    double sum = 0.0d;
                    for (int i = 0; i < lambdas.length; i++) {
                        sum += lambdas[i];
                    }
                    if (Math.abs(sum - 1.0) > 0.001) {
                        throw new ElasticsearchIllegalArgumentException("linear smoothing lambdas must sum to 1");
                    }
                    suggestion.setModel(new WordScorer.WordScorerFactory() {
                        @Override
                        public WordScorer newScorer(IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator)
                                throws IOException {
                            return new LinearInterpoatingScorer(reader, terms, field, realWordLikelyhood, separator, lambdas[0], lambdas[1],
                                    lambdas[2]);
                        }
                    });
                } else if ("laplace".equals(fieldName)) {
                    ensureNoSmoothing(suggestion);
                    double theAlpha = 0.5;

                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        }
                        if (token.isValue() && "alpha".equals(fieldName)) {
                            theAlpha = parser.doubleValue();
                        }
                    }
                    final double alpha = theAlpha;
                    suggestion.setModel(new WordScorer.WordScorerFactory() {
                        @Override
                        public WordScorer newScorer(IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator)
                                throws IOException {
                            return new LaplaceScorer(reader, terms,  field, realWordLikelyhood, separator, alpha);
                        }
                    });

                } else if ("stupid_backoff".equals(fieldName) || "stupidBackoff".equals(fieldName)) {
                    ensureNoSmoothing(suggestion);
                    double theDiscount = 0.4;
                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        }
                        if (token.isValue() && "discount".equals(fieldName)) {
                            theDiscount = parser.doubleValue();
                        }
                    }
                    final double discount = theDiscount;
                    suggestion.setModel(new WordScorer.WordScorerFactory() {
                        @Override
                        public WordScorer newScorer(IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator)
                                throws IOException {
                            return new StupidBackoffScorer(reader, terms, field, realWordLikelyhood, separator, discount);
                        }
                    });

                } else {
                    throw new ElasticsearchIllegalArgumentException("suggester[phrase] doesn't support object field [" + fieldName + "]");
                }
            }
        }
    }

    private void ensureNoSmoothing(PhraseSuggestionContext suggestion) {
        if (suggestion.model() != null) {
            throw new ElasticsearchIllegalArgumentException("only one smoothing model supported");
        }
    }

    private void verifyGenerator(PhraseSuggestionContext.DirectCandidateGenerator suggestion) {
        // Verify options and set defaults
        if (suggestion.field() == null) {
            throw new ElasticsearchIllegalArgumentException("The required field option is missing");
        }
    }

    private void parseCandidateGenerator(XContentParser parser, MapperService mapperService, String fieldName,
            PhraseSuggestionContext.DirectCandidateGenerator generator) throws IOException {
        if (!SuggestUtils.parseDirectSpellcheckerSettings(parser, fieldName, generator)) {
            if ("field".equals(fieldName)) {
                generator.setField(parser.text());
                if (mapperService.smartNameFieldMapper(generator.field()) == null) {
                    throw new ElasticsearchIllegalArgumentException("No mapping found for field [" + generator.field() + "]");
                }
            } else if ("size".equals(fieldName)) {
                generator.size(parser.intValue());
            } else if ("pre_filter".equals(fieldName) || "preFilter".equals(fieldName)) {
                String analyzerName = parser.text();
                Analyzer analyzer = mapperService.analysisService().analyzer(analyzerName);
                if (analyzer == null) {
                    throw new ElasticsearchIllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
                }
                generator.preFilter(analyzer);
            } else if ("post_filter".equals(fieldName) || "postFilter".equals(fieldName)) {
                String analyzerName = parser.text();
                Analyzer analyzer = mapperService.analysisService().analyzer(analyzerName);
                if (analyzer == null) {
                    throw new ElasticsearchIllegalArgumentException("Analyzer [" + analyzerName + "] doesn't exists");
                }
                generator.postFilter(analyzer);
            } else {
                throw new ElasticsearchIllegalArgumentException("CandidateGenerator doesn't support [" + fieldName + "]");
            }
        }
    }

}
