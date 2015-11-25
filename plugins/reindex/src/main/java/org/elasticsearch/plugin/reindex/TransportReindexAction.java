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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.index.VersionType.INTERNAL;

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, ReindexResponse> {
    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final AutoCreateIndex autoCreateIndex;
    private final Client client;

    @Inject
    public TransportReindexAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, ScriptService scriptService,
            AutoCreateIndex autoCreateIndex, Client client, TransportService transportService) {
        super(settings, ReindexAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                ReindexRequest::new);
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.autoCreateIndex = autoCreateIndex;
        this.client = client;
    }

    @Override
    protected void doExecute(ReindexRequest request, ActionListener<ReindexResponse> listener) {
        validateAgainstAliases(request.getSource(), request.getDestination(), indexNameExpressionResolver, autoCreateIndex,
                clusterService.state());
        new AsyncIndexBySearchAction(logger, scriptService, client, threadPool, request, listener).start();
    }

    /**
     * Throws an ActionRequestValidationException if the request tries to index
     * back into the same index or into an index that points to two indexes.
     * This cannot be done during request validation because the cluster state
     * isn't available then. Package private for testing.
     */
    static String validateAgainstAliases(SearchRequest source, IndexRequest destination,
            IndexNameExpressionResolver indexNameExpressionResolver, AutoCreateIndex autoCreateIndex, ClusterState clusterState) {
        String target = destination.index();
        if (false == autoCreateIndex.shouldAutoCreate(target, clusterState)) {
            /*
             * If we're going to autocreate the index we don't need to resolve
             * it. This is the same sort of dance that TransportIndexRequest
             * uses to decide to autocreate the index.
             */
            target = indexNameExpressionResolver.concreteIndices(clusterState, destination)[0];
        }
        for (String sourceIndex: indexNameExpressionResolver.concreteIndices(clusterState, source)) {
            if (sourceIndex.equals(target)) {
                ActionRequestValidationException e = new ActionRequestValidationException();
                e.addValidationError("reindex cannot write into an index its reading from [" + target + ']');
                throw e;
            }
        }
        return target;
    }

    /**
     * Simple implementation of reindex using scrolling and bulk. There are tons
     * of optimizations that can be done on certain types of reindex requests
     * but this makes no attempt to do any of them so it can be as simple
     * possible.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<ReindexRequest, ReindexResponse> {
        public AsyncIndexBySearchAction(ESLogger logger, ScriptService scriptService, Client client, ThreadPool threadPool,
                ReindexRequest request, ActionListener<ReindexResponse> listener) {
            super(logger, scriptService, client, threadPool, request, request.getSource(), listener);
        }

        @Override
        protected IndexRequest buildIndexRequest(SearchHit doc) {
            IndexRequest index = new IndexRequest(mainRequest.getDestination(), mainRequest);

            // We want the index from the copied request, not the doc.
            index.id(doc.id());
            if (index.type() == null) {
                /*
                 * Default to doc's type if not specified in request so its easy
                 * to do a scripted update.
                 */
                index.type(doc.type());
            }
            index.source(doc.sourceRef());
            /*
             * Internal versioning can just use what we copied from the
             * destionation request. Otherwise we assume we're using external
             * versioning and use the doc's version.
             */
            if (index.versionType() != INTERNAL) {
                index.version(doc.version());
            }
            return index;
        }

        /**
         * Override the simple copy behavior to allow more fine grained control.
         */
        @Override
        protected void copyRouting(IndexRequest index, SearchHit doc) {
            String routingSpec = mainRequest.getDestination().routing();
            if (routingSpec == null) {
                super.copyRouting(index, doc);
                return;
            }
            if (routingSpec.startsWith("=")) {
                index.routing(mainRequest.getDestination().routing().substring(1));
                return;
            }
            switch (routingSpec) {
            case "keep":
                super.copyRouting(index, doc);
                break;
            case "discard":
                index.routing(null);
                break;
            default:
                throw new IllegalArgumentException("Unsupported routing command");
            }
        }

        /*
         * Methods below here handle script updating the index request. They try
         * to be pretty liberal with regards to types because script are often
         * dynamically typed.
         */
        @Override
        protected ReindexResponse buildResponse(long took) {
            return new ReindexResponse(took, created(), updated(), batches(), versionConflicts(), noops(), indexingFailures(),
                    searchFailures());
        }

        @Override
        protected void scriptChangedIndex(IndexRequest index, Object to) {
            requireNonNull(to, "Can't reindex without a destination index!");
            index.index(to.toString());
        }

        @Override
        protected void scriptChangedType(IndexRequest index, Object to) {
            requireNonNull(to, "Can't reindex without a destination type!");
            index.type(to.toString());
        }

        @Override
        protected void scriptChangedId(IndexRequest index, Object to) {
            index.id(Objects.toString(to, null));
        }

        @Override
        protected void scriptChangedVersion(IndexRequest index, Object to) {
            if (to == null) {
                index.version(Versions.MATCH_ANY).versionType(INTERNAL);
                return;
            }
            index.version(asLong(to, VersionFieldMapper.NAME));
        }

        @Override
        protected void scriptChangedParent(IndexRequest index, Object to) {
            // Have to override routing with parent just in case its changed
            String routing = Objects.toString(to, null);
            index.parent(routing).routing(routing);
        }

        @Override
        protected void scriptChangedRouting(IndexRequest index, Object to) {
            index.routing(Objects.toString(to, null));
        }

        @Override
        protected void scriptChangedTimestamp(IndexRequest index, Object to) {
            index.timestamp(Objects.toString(to, null));
        }

        @Override
        protected void scriptChangedTTL(IndexRequest index, Object to) {
            if (to == null) {
                index.ttl(null);
                return;
            }
            index.ttl(asLong(to, TTLFieldMapper.NAME));
        }

        private long asLong(Object from, String name) {
            /*
             * Stuffing a number into the map will have converted it to
             * some Number.
             */
            Number fromNumber;
            try {
                fromNumber = (Number) from;
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(name + " may only be set to an int or a long but was [" + from + "]", e);
            }
            long l = fromNumber.longValue();
            // Check that we didn't round when we fetched the value.
            if (fromNumber.doubleValue() != l) {
                throw new IllegalArgumentException(name + " may only be set to an int or a long but was [" + from + "]");
            }
            return l;
        }
    }
}
