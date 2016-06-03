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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.index.VersionType.INTERNAL;

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, BulkIndexByScrollResponse> {
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
    protected void doExecute(Task task, ReindexRequest request, ActionListener<BulkIndexByScrollResponse> listener) {
        ClusterState state = clusterService.state();
        validateAgainstAliases(request.getSearchRequest(), request.getDestination(), indexNameExpressionResolver, autoCreateIndex, state);
        ParentTaskAssigningClient client = new ParentTaskAssigningClient(this.client, clusterService.localNode(), task);
        new AsyncIndexBySearchAction((BulkByScrollTask) task, logger, client, threadPool, request, listener, scriptService, state).start();
    }

    @Override
    protected void doExecute(ReindexRequest request, ActionListener<BulkIndexByScrollResponse> listener) {
        throw new UnsupportedOperationException("task required");
    }

    /**
     * Throws an ActionRequestValidationException if the request tries to index
     * back into the same index or into an index that points to two indexes.
     * This cannot be done during request validation because the cluster state
     * isn't available then. Package private for testing.
     */
    static String validateAgainstAliases(SearchRequest source, IndexRequest destination,
                                         IndexNameExpressionResolver indexNameExpressionResolver, AutoCreateIndex autoCreateIndex,
                                         ClusterState clusterState) {
        String target = destination.index();
        if (false == autoCreateIndex.shouldAutoCreate(target, clusterState)) {
            /*
             * If we're going to autocreate the index we don't need to resolve
             * it. This is the same sort of dance that TransportIndexRequest
             * uses to decide to autocreate the index.
             */
            target = indexNameExpressionResolver.concreteIndexNames(clusterState, destination)[0];
        }
        for (String sourceIndex : indexNameExpressionResolver.concreteIndexNames(clusterState, source)) {
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
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<ReindexRequest> {

        public AsyncIndexBySearchAction(BulkByScrollTask task, ESLogger logger, ParentTaskAssigningClient client, ThreadPool threadPool,
                                        ReindexRequest request, ActionListener<BulkIndexByScrollResponse> listener,
                                        ScriptService scriptService, ClusterState clusterState) {
            super(task, logger, client, threadPool, request, request.getSearchRequest(), listener, scriptService, clusterState);
        }

        @Override
        protected BiFunction<RequestWrapper<?>, SearchHit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                return new ReindexScriptApplier(task, scriptService, script, clusterState, script.getParams());
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(SearchHit doc) {
            IndexRequest index = new IndexRequest();

            // Copy the index from the request so we always write where it asked to write
            index.index(mainRequest.getDestination().index());

            // If the request override's type then the user wants all documents in that type. Otherwise keep the doc's type.
            if (mainRequest.getDestination().type() == null) {
                index.type(doc.type());
            } else {
                index.type(mainRequest.getDestination().type());
            }

            /*
             * Internal versioning can just use what we copied from the destination request. Otherwise we assume we're using external
             * versioning and use the doc's version.
             */
            index.versionType(mainRequest.getDestination().versionType());
            if (index.versionType() == INTERNAL) {
                index.version(mainRequest.getDestination().version());
            } else {
                index.version(doc.version());
            }

            // id and source always come from the found doc. Scripts can change them but they operate on the index request.
            index.id(doc.id());
            index.source(doc.sourceRef());

            /*
             * The rest of the index request just has to be copied from the template. It may be changed later from scripts or the superclass
             * here on out operates on the index request rather than the template.
             */
            index.routing(mainRequest.getDestination().routing());
            index.parent(mainRequest.getDestination().parent());
            index.timestamp(mainRequest.getDestination().timestamp());
            index.ttl(mainRequest.getDestination().ttl());
            index.contentType(mainRequest.getDestination().getContentType());
            index.setPipeline(mainRequest.getDestination().getPipeline());
            // OpType is synthesized from version so it is handled when we copy version above.

            return wrap(index);
        }

        /**
         * Override the simple copy behavior to allow more fine grained control.
         */
        @Override
        protected void copyRouting(RequestWrapper<?> request, String routing) {
            String routingSpec = mainRequest.getDestination().routing();
            if (routingSpec == null) {
                super.copyRouting(request, routing);
                return;
            }
            if (routingSpec.startsWith("=")) {
                super.copyRouting(request, mainRequest.getDestination().routing().substring(1));
                return;
            }
            switch (routingSpec) {
            case "keep":
                super.copyRouting(request, routing);
                break;
            case "discard":
                super.copyRouting(request, null);
                break;
            default:
                throw new IllegalArgumentException("Unsupported routing command");
            }
        }

        class ReindexScriptApplier extends ScriptApplier {

            ReindexScriptApplier(BulkByScrollTask task, ScriptService scriptService, Script script, ClusterState state,
                                 Map<String, Object> params) {
                super(task, scriptService, script, state, params);
            }

            /*
             * Methods below here handle script updating the index request. They try
             * to be pretty liberal with regards to types because script are often
             * dynamically typed.
             */

            @Override
            protected void scriptChangedIndex(RequestWrapper<?> request, Object to) {
                requireNonNull(to, "Can't reindex without a destination index!");
                request.setIndex(to.toString());
            }

            @Override
            protected void scriptChangedType(RequestWrapper<?> request, Object to) {
                requireNonNull(to, "Can't reindex without a destination type!");
                request.setType(to.toString());
            }

            @Override
            protected void scriptChangedId(RequestWrapper<?> request, Object to) {
                request.setId(Objects.toString(to, null));
            }

            @Override
            protected void scriptChangedVersion(RequestWrapper<?> request, Object to) {
                if (to == null) {
                    request.setVersion(Versions.MATCH_ANY);
                    request.setVersionType(INTERNAL);
                } else {
                    request.setVersion(asLong(to, VersionFieldMapper.NAME));
                }
            }

            @Override
            protected void scriptChangedParent(RequestWrapper<?> request, Object to) {
                // Have to override routing with parent just in case its changed
                String routing = Objects.toString(to, null);
                request.setParent(routing);
                request.setRouting(routing);
            }

            @Override
            protected void scriptChangedRouting(RequestWrapper<?> request, Object to) {
                request.setRouting(Objects.toString(to, null));
            }

            @Override
            protected void scriptChangedTimestamp(RequestWrapper<?> request, Object to) {
                request.setTimestamp(Objects.toString(to, null));
            }

            @Override
            protected void scriptChangedTTL(RequestWrapper<?> request, Object to) {
                if (to == null) {
                    request.setTtl(null);
                } else {
                    request.setTtl(asLong(to, TTLFieldMapper.NAME));
                }
            }

            private long asLong(Object from, String name) {
                /*
                 * Stuffing a number into the map will have converted it to
                 * some Number.
                 * */
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
}
