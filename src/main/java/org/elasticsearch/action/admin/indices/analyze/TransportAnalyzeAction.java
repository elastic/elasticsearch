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

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.custom.TransportSingleCustomOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport action used to execute analyze requests
 */
public class TransportAnalyzeAction extends TransportSingleCustomOperationAction<AnalyzeRequest, AnalyzeResponse> {

    private final IndicesService indicesService;

    private final IndicesAnalysisService indicesAnalysisService;

    private static final Settings DEFAULT_SETTINGS = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();

    @Inject
    public TransportAnalyzeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                  IndicesService indicesService, IndicesAnalysisService indicesAnalysisService, ActionFilters actionFilters) {
        super(settings, AnalyzeAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.indicesService = indicesService;
        this.indicesAnalysisService = indicesAnalysisService;
        transportService.registerHandler(AnalyzeAction.NAME, new TransportHandler());
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
    protected AnalyzeResponse shardOperation(AnalyzeRequest request, ShardId shardId) throws ElasticsearchException {
        IndexService indexService = null;
        if (shardId != null) {
            indexService = indicesService.indexServiceSafe(shardId.getIndex());
        }
        AnalyzeContext context = parseSource(request);
        Analyzer analyzer = null;
        boolean closeAnalyzer = false;
        String field = null;
        if (context.field() != null) {
            if (indexService == null) {
                throw new ElasticsearchIllegalArgumentException("No index provided, and trying to analyzer based on a specific field which requires the index parameter");
            }
            FieldMapper<?> fieldMapper = indexService.mapperService().smartNameFieldMapper(context.field());
            if (fieldMapper != null) {
                if (fieldMapper.isNumeric()) {
                    throw new ElasticsearchIllegalArgumentException("Can't process field [" + context.field() + "], Analysis requests are not supported on numeric fields");
                }
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
        if (analyzer == null && context.analyzer() != null) {
            if (indexService == null) {
                analyzer = indicesAnalysisService.analyzer(context.analyzer());
            } else {
                analyzer = indexService.analysisService().analyzer(context.analyzer());
            }
            if (analyzer == null) {
                throw new ElasticsearchIllegalArgumentException("failed to find analyzer [" + context.analyzer() + "]");
            }
        } else if (context.tokenizer() != null) {
            TokenizerFactory tokenizerFactory;
            if (indexService == null) {
                TokenizerFactoryFactory tokenizerFactoryFactory = indicesAnalysisService.tokenizerFactoryFactory(context.tokenizer());
                if (tokenizerFactoryFactory == null) {
                    throw new ElasticsearchIllegalArgumentException("failed to find global tokenizer under [" + context.tokenizer() + "]");
                }
                tokenizerFactory = tokenizerFactoryFactory.create(context.tokenizer(), DEFAULT_SETTINGS);
            } else {
                tokenizerFactory = indexService.analysisService().tokenizer(context.tokenizer());
                if (tokenizerFactory == null) {
                    throw new ElasticsearchIllegalArgumentException("failed to find tokenizer under [" + context.tokenizer() + "]");
                }
            }

            TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[0];
            if (context.tokenFilters() != null && context.tokenFilters().isEmpty() == false) {
                tokenFilterFactories = new TokenFilterFactory[context.tokenFilters().size()];
                for (int i = 0; i < context.tokenFilters().size(); i++) {
                    String tokenFilterName = context.tokenFilters().get(i);
                    if (indexService == null) {
                        TokenFilterFactoryFactory tokenFilterFactoryFactory = indicesAnalysisService.tokenFilterFactoryFactory(tokenFilterName);
                        if (tokenFilterFactoryFactory == null) {
                            throw new ElasticsearchIllegalArgumentException("failed to find global token filter under [" + tokenFilterName + "]");
                        }
                        tokenFilterFactories[i] = tokenFilterFactoryFactory.create(tokenFilterName, DEFAULT_SETTINGS);
                    } else {
                        tokenFilterFactories[i] = indexService.analysisService().tokenFilter(tokenFilterName);
                        if (tokenFilterFactories[i] == null) {
                            throw new ElasticsearchIllegalArgumentException("failed to find token filter under [" + tokenFilterName + "]");
                        }
                    }
                    if (tokenFilterFactories[i] == null) {
                        throw new ElasticsearchIllegalArgumentException("failed to find token filter under [" + tokenFilterName + "]");
                    }
                }
            }

            CharFilterFactory[] charFilterFactories = new CharFilterFactory[0];
            if (context.charFilters() != null && context.charFilters().isEmpty() == false) {
                charFilterFactories = new CharFilterFactory[context.charFilters().size()];
                for (int i = 0; i < context.charFilters().size(); i++) {
                    String charFilterName = context.charFilters().get(i);
                    if (indexService == null) {
                        CharFilterFactoryFactory charFilterFactoryFactory = indicesAnalysisService.charFilterFactoryFactory(charFilterName);
                        if (charFilterFactoryFactory == null) {
                            throw new ElasticsearchIllegalArgumentException("failed to find global char filter under [" + charFilterName + "]");
                        }
                        charFilterFactories[i] = charFilterFactoryFactory.create(charFilterName, DEFAULT_SETTINGS);
                    } else {
                        charFilterFactories[i] = indexService.analysisService().charFilter(charFilterName);
                        if (charFilterFactories[i] == null) {
                            throw new ElasticsearchIllegalArgumentException("failed to find token char under [" + charFilterName + "]");
                        }
                    }
                    if (charFilterFactories[i] == null) {
                        throw new ElasticsearchIllegalArgumentException("failed to find token char under [" + charFilterName + "]");
                    }
                }
            }

            analyzer = new CustomAnalyzer(tokenizerFactory, charFilterFactories, tokenFilterFactories);
            closeAnalyzer = true;
        } else if (analyzer == null) {
            if (indexService == null) {
                analyzer = indicesAnalysisService.analyzer("standard");
            } else {
                analyzer = indexService.analysisService().defaultIndexAnalyzer();
            }
        }
        if (analyzer == null) {
            throw new ElasticsearchIllegalArgumentException("failed to find analyzer");
        }

        List<AnalyzeResponse.AnalyzeToken> tokens = Lists.newArrayList();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream(field, context.text());
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
            throw new ElasticsearchException("failed to analyze", e);
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


    private AnalyzeContext parseSource(AnalyzeRequest request) {
        AnalyzeContext context = new AnalyzeContext();
        try {
            Map<String, Object> contentMap = XContentHelper.convertToMap(request.source(), false).v2();
            for (Map.Entry<String, Object> entry : contentMap.entrySet()) {
                String name = entry.getKey();
                if ("prefer_local".equals(name)) {
                    request.preferLocal(XContentMapValues.nodeBooleanValue(entry.getValue(), request.preferLocalShard()));
                } else if ("analyzer".equals(name)) {
                    context.analyzer(XContentMapValues.nodeStringValue(entry.getValue(), null));
                } else if ("field".equals(name)) {
                    context.field(XContentMapValues.nodeStringValue(entry.getValue(), null));
                } else if ("tokenizer".equals(name)) {
                    context.tokenizer(XContentMapValues.nodeStringValue(entry.getValue(), null));
                } else if ("token_filters".equals(name) || "filters".equals(name)) {
                    if (XContentMapValues.isArray(entry.getValue())) {
                        context.tokenFilters((List<String>) entry.getValue());
                    }
                } else if ("char_filters".equals(name)) {
                    if (XContentMapValues.isArray(entry.getValue())) {
                        context.charFilters((List<String>) entry.getValue());
                    }
                } else if ("text".equals(name)) {
                    context.text(XContentMapValues.nodeStringValue(entry.getValue(), null));
                }
            }
        } catch (ElasticsearchParseException e) {
            throw new ElasticsearchIllegalArgumentException("Failed to parse request body", e);
        }
        if (context.text() == null) {
            throw new ElasticsearchIllegalArgumentException("text is missing");
        }
        return context;
    }

    private class AnalyzeContext {
        private String text;
        private String analyzer;
        private String tokenizer;
        private List<String> tokenFilters = Lists.newArrayList();
        private List<String> charFilters = Lists.newArrayList();
        private String field;

        public String text() {
            return text;
        }

        public String analyzer() {
            return analyzer;
        }

        public String tokenizer() {
            return tokenizer;
        }

        public List<String> tokenFilters() {
            return tokenFilters;
        }

        public List<String> charFilters() {
            return charFilters;
        }

        public String field() {
            return field;
        }

        public void text(String text) {
            this.text = text;
        }

        public void analyzer(String analyzer) {
            this.analyzer = analyzer;
        }

        public void tokenizer(String tokenizer) {
            this.tokenizer = tokenizer;
        }

        public void tokenFilters(List<String> tokenFilters) {
            this.tokenFilters = tokenFilters;
        }

        public void charFilters(List<String> charFilters) {
            this.charFilters = charFilters;
        }

        public void field(String field) {
            this.field = field;
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<AnalyzeRequest> {

        @Override
        public AnalyzeRequest newInstance() {
            return newRequest();
        }

        @Override
        public void messageReceived(AnalyzeRequest request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            // if we have a local operation, execute it on a thread since we don't spawn
            request.operationThreaded(true);
            execute(request, new ActionListener<AnalyzeResponse>() {
                @Override
                public void onResponse(AnalyzeResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
