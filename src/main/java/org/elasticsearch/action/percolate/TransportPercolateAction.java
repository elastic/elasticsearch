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

package org.elasticsearch.action.percolate;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.percolator.PercolateException;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportPercolateAction extends TransportBroadcastOperationAction<PercolateRequest, PercolateResponse, PercolateShardRequest, PercolateShardResponse> {

    private final PercolatorService percolatorService;
    private final TransportGetAction getAction;

    @Inject
    public TransportPercolateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                    TransportService transportService, PercolatorService percolatorService,
                                    TransportGetAction getAction) {
        super(settings, threadPool, clusterService, transportService);
        this.percolatorService = percolatorService;
        this.getAction = getAction;
    }

    @Override
    protected void doExecute(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        resolveGet(request, listener);
    }

    // Add redirect here if a request ends up on a non data node? In the case when percolating an existing doc this
    // could be beneficial.
    void resolveGet(PercolateRequest originalRequest, ActionListener<PercolateResponse> listener) {
        originalRequest.startTime = System.currentTimeMillis();
        BytesReference body = originalRequest.source();
        Tuple<GetRequest, Long> tuple = null;

        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(body).createParser(body);
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticSearchParseException("percolate request didn't start with start object");
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    // we need to check the "doc" here, so the next token will be START_OBJECT which is
                    // the actual document starting
                    if ("doc".equals(currentFieldName)) {
                        parser.close();
                        super.doExecute(originalRequest, listener);
                        return;
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("get".equals(currentFieldName)) {
                        tuple = createGetRequest(parser, originalRequest.indices()[0], originalRequest.documentType());
                        break;
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == null) {
                    break;
                } else {
                    parser.skipChildren();
                }
            }

            // docSource shouldn't be null
            assert tuple != null;
            executeGet(tuple, originalRequest, listener);
        } catch (IOException e) {
            throw new ElasticSearchParseException("failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    void executeGet(Tuple<GetRequest, Long> tuple, final PercolateRequest originalRequest, final ActionListener<PercolateResponse> listener) {
        final GetRequest getRequest = tuple.v1();
        final Long getVersion = tuple.v2();
        getAction.execute(tuple.v1(), new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (!getResponse.isExists()) {
                    onFailure(new DocumentMissingException(null, getRequest.type(), getRequest.id()));
                    return;
                }

                if (getVersion != null && getVersion != getResponse.getVersion()) {
                    onFailure(new VersionConflictEngineException(null, getRequest.type(), getRequest.id(), getResponse.getVersion(), getVersion));
                    return;
                }
                BytesReference fetchedSource = getResponse.getSourceAsBytesRef();
                TransportPercolateAction.super.doExecute(new PercolateRequest(originalRequest, fetchedSource), listener);
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    Tuple<GetRequest, Long> createGetRequest(XContentParser parser, String index, String type) throws IOException {
        String getCurrentField = null;
        String getIndex = index;
        String getType = type;
        String getId = null;
        Long getVersion = null;
        String getRouting = null;
        String getPreference = "_local";

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                getCurrentField = parser.currentName();
            } else if (token.isValue()) {
                if ("index".equals(getCurrentField)) {
                    getIndex = parser.text();
                } else if ("type".equals(getCurrentField)) {
                    getType = parser.text();
                } else if ("id".equals(getCurrentField)) {
                    getId = parser.text();
                } else if ("version".equals(getCurrentField)) {
                    getVersion = parser.longValue();
                } else if ("routing".equals(getCurrentField)) {
                    getRouting = parser.text();
                } else if ("preference".equals(getCurrentField)) {
                    getPreference = parser.text();
                }
            }
        }
        return new Tuple<GetRequest, Long>(
                // We are on the network thread, so operationThreaded should be true
                new GetRequest(getIndex).preference(getPreference).operationThreaded(true).type(getType).id(getId).routing(getRouting),
                getVersion
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.PERCOLATE;
    }

    @Override
    protected PercolateRequest newRequest() {
        return new PercolateRequest();
    }

    @Override
    protected String transportAction() {
        return PercolateAction.NAME;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PercolateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PercolateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected PercolateResponse newResponse(PercolateRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;

        List<Text[]> shardResults = null;
        List<ShardOperationFailedException> shardFailures = null;

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                PercolateShardResponse percolateShardResponse = (PercolateShardResponse) shardResponse;
                if (shardResults == null) {
                    shardResults = newArrayList();
                }
                shardResults.add(percolateShardResponse.matches());
                successfulShards++;
            }
        }

        long tookInMillis = System.currentTimeMillis() - request.startTime;
        if (shardResults == null) {
            return new PercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, tookInMillis);
        }

        int size = 0;
        for (Text[] shardResult : shardResults) {
            size += shardResult.length;
        }
        Text[] finalMatches = new Text[size];
        int offset = 0;
        for (Text[] shardResult : shardResults) {
            System.arraycopy(shardResult, 0, finalMatches, offset, shardResult.length);
            offset += shardResult.length;
        }
        assert size == offset;
        return new PercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, finalMatches, tookInMillis);
    }

    @Override
    protected PercolateShardRequest newShardRequest() {
        return new PercolateShardRequest();
    }

    @Override
    protected PercolateShardRequest newShardRequest(ShardRouting shard, PercolateRequest request) {
        return new PercolateShardRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected PercolateShardResponse newShardResponse() {
        return new PercolateShardResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, PercolateRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected PercolateShardResponse shardOperation(PercolateShardRequest request) throws ElasticSearchException {
        try {
            return percolatorService.percolate(request);
        } catch (Throwable t) {
            logger.trace("[{}][{}] failed to percolate", t, request.index(), request.shardId());
            ShardId shardId = new ShardId(request.index(), request.shardId());
            throw new PercolateException(shardId, "failed to percolate", t);
        }
    }

}
