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

package org.elasticsearch.action.admin.indices.analyze;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.support.single.custom.TransportSingleCustomOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TransportAnalyzeAction extends TransportSingleCustomOperationAction<AnalyzeRequest, AnalyzeResponse> {

    private final IndicesService indicesService;

    private final IndicesAnalysisService indicesAnalysisService;

    @Inject
    public TransportAnalyzeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                  IndicesService indicesService, IndicesAnalysisService indicesAnalysisService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.indicesAnalysisService = indicesAnalysisService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected AnalyzeRequest newRequest() {
        return new AnalyzeRequest();
    }

    @Override
    protected AnalyzeResponse newResponse() {
        return new AnalyzeResponse();
    }

    @Override
    protected String transportAction() {
        return AnalyzeAction.NAME;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, AnalyzeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, AnalyzeRequest request) {
        if (request.getIndex() != null) {
            request.setIndex(state.metaData().concreteIndex(request.getIndex()));
            return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.getIndex());
        }
        return null;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, AnalyzeRequest request) {
        if (request.getIndex() == null) {
            // just execute locally....
            return null;
        }
        return state.routingTable().index(request.getIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected AnalyzeResponse shardOperation(AnalyzeRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = null;
        if (request.getIndex() != null) {
            indexService = indicesService.indexServiceSafe(request.getIndex());
        }
        Analyzer analyzer = null;
        boolean closeAnalyzer = false;
        String field = null;
        if (request.getField() != null) {
            if (indexService == null) {
                throw new ElasticSearchIllegalArgumentException("No index provided, and trying to analyzer based on a specific field which requires the index parameter");
            }
            FieldMapper fieldMapper = indexService.mapperService().smartNameFieldMapper(request.getField());
            if (fieldMapper != null) {
                analyzer = fieldMapper.indexAnalyzer();
                field = fieldMapper.names().indexName();
            }
        }
        if (field == null) {
            if (indexService != null) {
                field = indexService.queryParserService().defaultField();
            } else {
                field = AllFieldMapper.NAME;
            }
        }
        if (analyzer == null && request.getAnalyzer() != null) {
            if (indexService == null) {
                analyzer = indicesAnalysisService.analyzer(request.getAnalyzer());
            } else {
                analyzer = indexService.analysisService().analyzer(request.getAnalyzer());
            }
            if (analyzer == null) {
                throw new ElasticSearchIllegalArgumentException("failed to find analyzer [" + request.getAnalyzer() + "]");
            }
        } else if (request.getTokenizer() != null) {
            TokenizerFactory tokenizerFactory;
            if (indexService == null) {
                TokenizerFactoryFactory tokenizerFactoryFactory = indicesAnalysisService.tokenizerFactoryFactory(request.getTokenizer());
                if (tokenizerFactoryFactory == null) {
                    throw new ElasticSearchIllegalArgumentException("failed to find global tokenizer under [" + request.getTokenizer() + "]");
                }
                tokenizerFactory = tokenizerFactoryFactory.create(request.getTokenizer(), ImmutableSettings.Builder.EMPTY_SETTINGS);
            } else {
                tokenizerFactory = indexService.analysisService().tokenizer(request.getTokenizer());
                if (tokenizerFactory == null) {
                    throw new ElasticSearchIllegalArgumentException("failed to find tokenizer under [" + request.getTokenizer() + "]");
                }
            }
            TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[0];
            if (request.getTokenFilters() != null && request.getTokenFilters().length > 0) {
                tokenFilterFactories = new TokenFilterFactory[request.getTokenFilters().length];
                for (int i = 0; i < request.getTokenFilters().length; i++) {
                    String tokenFilterName = request.getTokenFilters()[i];
                    if (indexService == null) {
                        TokenFilterFactoryFactory tokenFilterFactoryFactory = indicesAnalysisService.tokenFilterFactoryFactory(tokenFilterName);
                        if (tokenFilterFactoryFactory == null) {
                            throw new ElasticSearchIllegalArgumentException("failed to find global token filter under [" + request.getTokenizer() + "]");
                        }
                        tokenFilterFactories[i] = tokenFilterFactoryFactory.create(tokenFilterName, ImmutableSettings.Builder.EMPTY_SETTINGS);
                    } else {
                        tokenFilterFactories[i] = indexService.analysisService().tokenFilter(tokenFilterName);
                        if (tokenFilterFactories[i] == null) {
                            throw new ElasticSearchIllegalArgumentException("failed to find token filter under [" + request.getTokenizer() + "]");
                        }
                    }
                    if (tokenFilterFactories[i] == null) {
                        throw new ElasticSearchIllegalArgumentException("failed to find token filter under [" + request.getTokenizer() + "]");
                    }
                }
            }
            analyzer = new CustomAnalyzer(tokenizerFactory, new CharFilterFactory[0], tokenFilterFactories);
            closeAnalyzer = true;
        } else if (analyzer == null) {
            if (indexService == null) {
                analyzer = Lucene.STANDARD_ANALYZER;
            } else {
                analyzer = indexService.analysisService().defaultIndexAnalyzer();
            }
        }
        if (analyzer == null) {
            throw new ElasticSearchIllegalArgumentException("failed to find analyzer");
        }

        List<AnalyzeResponse.AnalyzeToken> tokens = Lists.newArrayList();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream(field, new FastStringReader(request.getText()));
            stream.reset();
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
            OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
            TypeAttribute type = stream.addAttribute(TypeAttribute.class);

            int position = 0;
            while (stream.incrementToken()) {
                int increment = posIncr.getPositionIncrement();
                if (increment > 0) {
                    position = position + increment;
                }
                tokens.add(new AnalyzeResponse.AnalyzeToken(term.toString(), position, offset.startOffset(), offset.endOffset(), type.type()));
            }
            stream.end();
        } catch (IOException e) {
            throw new ElasticSearchException("failed to analyze", e);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (closeAnalyzer) {
                analyzer.close();
            }
        }

        return new AnalyzeResponse(tokens);
    }
}
