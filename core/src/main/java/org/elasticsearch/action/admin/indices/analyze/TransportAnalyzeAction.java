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
package org.elasticsearch.action.admin.indices.analyze;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport action used to execute analyze requests
 */
public class TransportAnalyzeAction extends TransportSingleShardAction<AnalyzeRequest, AnalyzeResponse> {

    private final IndicesService indicesService;
    private final Environment environment;

    @Inject
    public TransportAnalyzeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                  IndicesService indicesService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver, Environment environment) {
        super(settings, AnalyzeAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, AnalyzeRequest::new, ThreadPool.Names.INDEX);
        this.indicesService = indicesService;
        this.environment = environment;
    }

    @Override
    protected AnalyzeResponse newResponse() {
        return new AnalyzeResponse();
    }

    @Override
    protected boolean resolveIndex(AnalyzeRequest request) {
        return request.index() != null;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        if (request.concreteIndex() != null) {
            return super.checkRequestBlock(state, request);
        }
        return null;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        if (request.concreteIndex() == null) {
            // just execute locally....
            return null;
        }
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected AnalyzeResponse shardOperation(AnalyzeRequest request, ShardId shardId) {
        try {
            final IndexService indexService;
            if (shardId != null) {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } else {
                indexService = null;
            }
            String field = null;
            Analyzer analyzer = null;
            if (request.field() != null) {
                if (indexService == null) {
                    throw new IllegalArgumentException("No index provided, and trying to analyzer based on a specific field which requires the index parameter");
                }
                MappedFieldType fieldType = indexService.mapperService().smartNameFieldType(request.field());
                if (fieldType != null) {
                    if (fieldType.isNumeric()) {
                        throw new IllegalArgumentException("Can't process field [" + request.field() + "], Analysis requests are not supported on numeric fields");
                    }
                    analyzer = fieldType.indexAnalyzer();
                    field = fieldType.names().indexName();
                }
            }
            if (field == null) {
                if (indexService != null) {
                    field = indexService.getIndexSettings().getDefaultField();
                } else {
                    field = AllFieldMapper.NAME;
                }
            }
            final AnalysisRegistry analysisRegistry = indicesService.getAnalysis();
            return analyze(request, field, analyzer, indexService != null ? indexService.analysisService() : null, analysisRegistry, environment);
        } catch (IOException e) {
            throw new ElasticsearchException("analysis failed", e);
        }

    }

    public static AnalyzeResponse analyze(AnalyzeRequest request, String field,  Analyzer analyzer, AnalysisService analysisService, AnalysisRegistry analysisRegistry, Environment environment) throws IOException {

        boolean closeAnalyzer = false;
        if (analyzer == null && request.analyzer() != null) {
            if (analysisService == null) {
                analyzer = analysisRegistry.getAnalyzer(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find global analyzer [" + request.analyzer() + "]");
                }
            } else {
                analyzer = analysisService.analyzer(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find analyzer [" + request.analyzer() + "]");
                }
            }

        } else if (request.tokenizer() != null) {
            TokenizerFactory tokenizerFactory;
            if (analysisService == null) {
                AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory = analysisRegistry.getTokenizerProvider(request.tokenizer());
                if (tokenizerFactoryFactory == null) {
                    throw new IllegalArgumentException("failed to find global tokenizer under [" + request.tokenizer() + "]");
                }
                tokenizerFactory = tokenizerFactoryFactory.get(environment, request.tokenizer());
            } else {
                tokenizerFactory = analysisService.tokenizer(request.tokenizer());
                if (tokenizerFactory == null) {
                    throw new IllegalArgumentException("failed to find tokenizer under [" + request.tokenizer() + "]");
                }
            }

            TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[0];
            if (request.tokenFilters() != null && request.tokenFilters().length > 0) {
                tokenFilterFactories = new TokenFilterFactory[request.tokenFilters().length];
                for (int i = 0; i < request.tokenFilters().length; i++) {
                    String tokenFilterName = request.tokenFilters()[i];
                    if (analysisService == null) {
                        AnalysisModule.AnalysisProvider<TokenFilterFactory> tokenFilterFactoryFactory = analysisRegistry.getTokenFilterProvider(tokenFilterName);
                        if (tokenFilterFactoryFactory == null) {
                            throw new IllegalArgumentException("failed to find global token filter under [" + tokenFilterName + "]");
                        }
                        tokenFilterFactories[i] = tokenFilterFactoryFactory.get(environment, tokenFilterName);
                    } else {
                        tokenFilterFactories[i] = analysisService.tokenFilter(tokenFilterName);
                        if (tokenFilterFactories[i] == null) {
                            throw new IllegalArgumentException("failed to find token filter under [" + tokenFilterName + "]");
                        }
                    }
                    if (tokenFilterFactories[i] == null) {
                        throw new IllegalArgumentException("failed to find token filter under [" + tokenFilterName + "]");
                    }
                }
            }

            CharFilterFactory[] charFilterFactories = new CharFilterFactory[0];
            if (request.charFilters() != null && request.charFilters().length > 0) {
                charFilterFactories = new CharFilterFactory[request.charFilters().length];
                for (int i = 0; i < request.charFilters().length; i++) {
                    String charFilterName = request.charFilters()[i];
                    if (analysisService == null) {
                        AnalysisModule.AnalysisProvider<CharFilterFactory> charFilterFactoryFactory = analysisRegistry.getCharFilterProvider(charFilterName);
                        if (charFilterFactoryFactory == null) {
                            throw new IllegalArgumentException("failed to find global char filter under [" + charFilterName + "]");
                        }
                        charFilterFactories[i] = charFilterFactoryFactory.get(environment, charFilterName);
                    } else {
                        charFilterFactories[i] = analysisService.charFilter(charFilterName);
                        if (charFilterFactories[i] == null) {
                            throw new IllegalArgumentException("failed to find char filter under [" + charFilterName + "]");
                        }
                    }
                    if (charFilterFactories[i] == null) {
                        throw new IllegalArgumentException("failed to find char filter under [" + charFilterName + "]");
                    }
                }
            }

            analyzer = new CustomAnalyzer(tokenizerFactory, charFilterFactories, tokenFilterFactories);
            closeAnalyzer = true;
        } else if (analyzer == null) {
            if (analysisService == null) {
                analyzer = analysisRegistry.getAnalyzer("standard");
            } else {
                analyzer = analysisService.defaultIndexAnalyzer();
            }
        }
        if (analyzer == null) {
            throw new IllegalArgumentException("failed to find analyzer");
        }

        List<AnalyzeResponse.AnalyzeToken> tokens = new ArrayList<>();
        int lastPosition = -1;
        int lastOffset = 0;
        for (String text : request.text()) {
            try (TokenStream stream = analyzer.tokenStream(field, text)) {
                stream.reset();
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
                OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
                TypeAttribute type = stream.addAttribute(TypeAttribute.class);

                while (stream.incrementToken()) {
                    int increment = posIncr.getPositionIncrement();
                    if (increment > 0) {
                        lastPosition = lastPosition + increment;
                    }
                    tokens.add(new AnalyzeResponse.AnalyzeToken(term.toString(), lastPosition, lastOffset + offset.startOffset(), lastOffset + offset.endOffset(), type.type()));

                }
                stream.end();
                lastOffset += offset.endOffset();
                lastPosition += posIncr.getPositionIncrement();

                lastPosition += analyzer.getPositionIncrementGap(field);
                lastOffset += analyzer.getOffsetGap(field);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            }
        }

        if (closeAnalyzer) {
            analyzer.close();
        }

        return new AnalyzeResponse(tokens);
    }
}
