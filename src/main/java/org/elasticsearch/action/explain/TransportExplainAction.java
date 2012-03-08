/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.explain;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportExplainAction extends TransportShardSingleOperationAction<ExplainRequest, ExplainResponse> {

    private IndicesService indicesService;

    @Inject
    public TransportExplainAction(Settings settings, ClusterService clusterService, TransportService transportService,
            IndicesService indicesService, ThreadPool threadPool) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.EXPLAIN;
    }

    @Override
    protected String transportAction() {
        return ExplainAction.NAME;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ExplainRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ExplainRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState state, ExplainRequest request) {
        return clusterService.operationRouting().getShards(clusterService.state(), request.index(), request.type(),
                request.id(), request.routing(), request.preference());
    }

    @Override
    protected void resolveRequest(ClusterState state, ExplainRequest request) {
        // update the routing (request#index here is possibly an alias)
        request.routing(state.metaData().resolveIndexRouting(request.routing(), request.index()));
        request.index(state.metaData().concreteIndex(request.index()));
    }

    @Override
    protected ExplainResponse shardOperation(ExplainRequest request, int shardId) throws ElasticSearchException {
        request.beforeStart();
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);

        if (request.refresh()) {
            indexShard.refresh(new Engine.Refresh(false));
        }

        Query query = null;
        if (request.source() != null && request.sourceLength() != 0) {
            try {
                IndexQueryParserService queryParserService = indexService.queryParserService();
                XContentParser parser = XContentFactory.xContent(request.source(), request.sourceOffset(),
                        request.sourceLength()).createParser(request.source(), request.sourceOffset(),
                        request.sourceLength());
                XContentParser.Token token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ElasticSearchException("Failed to parse query, not starting with OBJECT");
                }
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("query".equals(currentFieldName)) {
                            query = queryParserService.parse(parser).query();
                            break;
                        } else {
                            parser.skipChildren();
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        parser.skipChildren();
                    }
                }
            } catch (IOException e) {
                throw new ElasticSearchException("Building the parser failed", e);
            }
        }
        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        Term term = UidFieldMapper.TERM_FACTORY.createTerm(Uid.createUid(request.type(), request.id()));
        Searcher searcher = indexShard.searcher();
        try {
            for (IndexReader reader : searcher.searcher().subReaders()) {
                UidField.DocIdAndVersion docIdAndVersion = UidField.loadDocIdAndVersion(reader, term);
                if (docIdAndVersion != null && docIdAndVersion.docId != Lucene.NO_DOC) {
                    return new ExplainResponse(request.index(), request.type(), request.id(), true, searcher.searcher()
                            .explain(query, docIdAndVersion.docId));
                }
            }
        } catch (IOException e) {
            throw new ElasticSearchException("Explain failed", e);
        } finally {
            searcher.release();
        }
        return new ExplainResponse(request.index(), request.type(), request.id(), false, null);
    }

    @Override
    protected ExplainRequest newRequest() {
        return new ExplainRequest();
    }

    @Override
    protected ExplainResponse newResponse() {
        return new ExplainResponse();
    }

}
