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

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.byscroll.AbstractAsyncBulkByScrollAction;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollParallelizationHelper;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.ParentBulkByScrollTask;
import org.elasticsearch.action.bulk.byscroll.ScrollableHitSource;
import org.elasticsearch.action.bulk.byscroll.ScrollableHitSource.SearchFailure;
import org.elasticsearch.action.bulk.byscroll.WorkingBulkByScrollTask;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.reindex.remote.RemoteInfo;
import org.elasticsearch.index.reindex.remote.RemoteScrollableHitSource;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedList;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.index.VersionType.INTERNAL;

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, BulkByScrollResponse> {
    public static final Setting<List<String>> REMOTE_CLUSTER_WHITELIST =
            Setting.listSetting("reindex.remote.whitelist", emptyList(), Function.identity(), Property.NodeScope);

    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final AutoCreateIndex autoCreateIndex;
    private final Client client;
    private final CharacterRunAutomaton remoteWhitelist;

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
        remoteWhitelist = buildRemoteWhitelist(REMOTE_CLUSTER_WHITELIST.get(settings));
    }

    @Override
    protected void doExecute(Task task, ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        if (request.getSlices() > 1) {
            BulkByScrollParallelizationHelper.startSlices(client, taskManager, ReindexAction.INSTANCE, clusterService.localNode().getId(),
                    (ParentBulkByScrollTask) task, request, listener);
        } else {
            checkRemoteWhitelist(remoteWhitelist, request.getRemoteInfo());
            ClusterState state = clusterService.state();
            validateAgainstAliases(request.getSearchRequest(), request.getDestination(), request.getRemoteInfo(),
                    indexNameExpressionResolver, autoCreateIndex, state);
            ParentTaskAssigningClient client = new ParentTaskAssigningClient(this.client, clusterService.localNode(), task);
            new AsyncIndexBySearchAction((WorkingBulkByScrollTask) task, logger, client, threadPool, request, scriptService, state,
                    listener).start();
        }
    }

    @Override
    protected void doExecute(ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        throw new UnsupportedOperationException("task required");
    }

    static void checkRemoteWhitelist(CharacterRunAutomaton whitelist, RemoteInfo remoteInfo) {
        if (remoteInfo == null) {
            return;
        }
        String check = remoteInfo.getHost() + ':' + remoteInfo.getPort();
        if (whitelist.run(check)) {
            return;
        }
        throw new IllegalArgumentException('[' + check + "] not whitelisted in " + REMOTE_CLUSTER_WHITELIST.getKey());
    }

    /**
     * Build the {@link CharacterRunAutomaton} that represents the reindex-from-remote whitelist and make sure that it doesn't whitelist
     * the world.
     */
    static CharacterRunAutomaton buildRemoteWhitelist(List<String> whitelist) {
        if (whitelist.isEmpty()) {
            return new CharacterRunAutomaton(Automata.makeEmpty());
        }
        Automaton automaton = Regex.simpleMatchToAutomaton(whitelist.toArray(Strings.EMPTY_ARRAY));
        automaton = MinimizationOperations.minimize(automaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
        if (Operations.isTotal(automaton)) {
            throw new IllegalArgumentException("Refusing to start because whitelist " + whitelist + " accepts all addresses. "
                    + "This would allow users to reindex-from-remote any URL they like effectively having Elasticsearch make HTTP GETs "
                    + "for them.");
        }
        return new CharacterRunAutomaton(automaton);
    }

    /**
     * Throws an ActionRequestValidationException if the request tries to index
     * back into the same index or into an index that points to two indexes.
     * This cannot be done during request validation because the cluster state
     * isn't available then. Package private for testing.
     */
    static void validateAgainstAliases(SearchRequest source, IndexRequest destination, RemoteInfo remoteInfo,
                                         IndexNameExpressionResolver indexNameExpressionResolver, AutoCreateIndex autoCreateIndex,
                                         ClusterState clusterState) {
        if (remoteInfo != null) {
            return;
        }
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
    }

    /**
     * Build the {@link RestClient} used for reindexing from remote clusters.
     * @param remoteInfo connection information for the remote cluster
     * @param taskId the id of the current task. This is added to the thread name for easier tracking
     * @param threadCollector a list in which we collect all the threads created by the client
     */
    static RestClient buildRestClient(RemoteInfo remoteInfo, long taskId, List<Thread> threadCollector) {
        Header[] clientHeaders = new Header[remoteInfo.getHeaders().size()];
        int i = 0;
        for (Map.Entry<String, String> header : remoteInfo.getHeaders().entrySet()) {
            clientHeaders[i++] = new BasicHeader(header.getKey(), header.getValue());
        }
        return RestClient.builder(new HttpHost(remoteInfo.getHost(), remoteInfo.getPort(), remoteInfo.getScheme()))
                .setDefaultHeaders(clientHeaders)
                .setRequestConfigCallback(c -> {
                    c.setConnectTimeout(Math.toIntExact(remoteInfo.getConnectTimeout().millis()));
                    c.setSocketTimeout(Math.toIntExact(remoteInfo.getSocketTimeout().millis()));
                    return c;
                })
                .setHttpClientConfigCallback(c -> {
                    // Enable basic auth if it is configured
                    if (remoteInfo.getUsername() != null) {
                        UsernamePasswordCredentials creds = new UsernamePasswordCredentials(remoteInfo.getUsername(),
                                remoteInfo.getPassword());
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY, creds);
                        c.setDefaultCredentialsProvider(credentialsProvider);
                    }
                    // Stick the task id in the thread name so we can track down tasks from stack traces
                    AtomicInteger threads = new AtomicInteger();
                    c.setThreadFactory(r -> {
                        String name = "es-client-" + taskId + "-" + threads.getAndIncrement();
                        Thread t = new Thread(r, name);
                        threadCollector.add(t);
                        return t;
                    });
                    // Limit ourselves to one reactor thread because for now the search process is single threaded.
                    c.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build());
                    return c;
                }).build();
    }

    /**
     * Simple implementation of reindex using scrolling and bulk. There are tons
     * of optimizations that can be done on certain types of reindex requests
     * but this makes no attempt to do any of them so it can be as simple
     * possible.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByScrollAction<ReindexRequest> {
        /**
         * List of threads created by this process. Usually actions don't create threads in Elasticsearch. Instead they use the builtin
         * {@link ThreadPool}s. But reindex-from-remote uses Elasticsearch's {@link RestClient} which doesn't use the
         * {@linkplain ThreadPool}s because it uses httpasyncclient. It'd be a ton of trouble to work around creating those threads. So
         * instead we let it create threads but we watch them carefully and assert that they are dead when the process is over.
         */
        private List<Thread> createdThreads = emptyList();

        AsyncIndexBySearchAction(WorkingBulkByScrollTask task, Logger logger, ParentTaskAssigningClient client,
                ThreadPool threadPool, ReindexRequest request, ScriptService scriptService, ClusterState clusterState,
                ActionListener<BulkByScrollResponse> listener) {
            super(task, logger, client, threadPool, request, scriptService, clusterState, listener);
        }

        @Override
        protected boolean needsSourceDocumentVersions() {
            /*
             * We only need the source version if we're going to use it when write and we only do that when the destination request uses
             * external versioning.
             */
            return mainRequest.getDestination().versionType() != VersionType.INTERNAL;
        }

        @Override
        protected ScrollableHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy) {
            if (mainRequest.getRemoteInfo() != null) {
                RemoteInfo remoteInfo = mainRequest.getRemoteInfo();
                createdThreads = synchronizedList(new ArrayList<>());
                RestClient restClient = buildRestClient(remoteInfo, task.getId(), createdThreads);
                return new RemoteScrollableHitSource(logger, backoffPolicy, threadPool, task::countSearchRetry, this::finishHim, restClient,
                        remoteInfo.getQuery(), mainRequest.getSearchRequest());
            }
            return super.buildScrollableResultSource(backoffPolicy);
        }

        @Override
        protected void finishHim(Exception failure, List<Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
            super.finishHim(failure, indexingFailures, searchFailures, timedOut);
            // A little extra paranoia so we log something if we leave any threads running
            for (Thread thread : createdThreads) {
                if (thread.isAlive()) {
                    assert false: "Failed to properly stop client thread [" + thread.getName() + "]";
                    logger.error("Failed to properly stop client thread [{}]", thread.getName());
                }
            }
        }

        @Override
        public BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                return new ReindexScriptApplier(task, scriptService, script, script.getParams());
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(ScrollableHitSource.Hit doc) {
            IndexRequest index = new IndexRequest();

            // Copy the index from the request so we always write where it asked to write
            index.index(mainRequest.getDestination().index());

            // If the request override's type then the user wants all documents in that type. Otherwise keep the doc's type.
            if (mainRequest.getDestination().type() == null) {
                index.type(doc.getType());
            } else {
                index.type(mainRequest.getDestination().type());
            }

            /*
             * Internal versioning can just use what we copied from the destination request. Otherwise we assume we're using external
             * versioning and use the doc's version.
             */
            index.versionType(mainRequest.getDestination().versionType());
            if (index.versionType() == INTERNAL) {
                assert doc.getVersion() == -1 : "fetched version when we didn't have to";
                index.version(mainRequest.getDestination().version());
            } else {
                index.version(doc.getVersion());
            }

            // id and source always come from the found doc. Scripts can change them but they operate on the index request.
            index.id(doc.getId());

            // the source xcontent type and destination could be different
            final XContentType sourceXContentType = doc.getXContentType();
            final XContentType mainRequestXContentType = mainRequest.getDestination().getContentType();
            if (mainRequestXContentType != null && doc.getXContentType() != mainRequestXContentType) {
                // we need to convert
                try (XContentParser parser = sourceXContentType.xContent().createParser(NamedXContentRegistry.EMPTY, doc.getSource());
                     XContentBuilder builder = XContentBuilder.builder(mainRequestXContentType.xContent())) {
                    parser.nextToken();
                    builder.copyCurrentStructure(parser);
                    index.source(builder.bytes(), builder.contentType());
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to convert hit from " + sourceXContentType + " to "
                        + mainRequestXContentType, e);
                }
            } else {
                index.source(doc.getSource(), doc.getXContentType());
            }

            /*
             * The rest of the index request just has to be copied from the template. It may be changed later from scripts or the superclass
             * here on out operates on the index request rather than the template.
             */
            index.routing(mainRequest.getDestination().routing());
            index.parent(mainRequest.getDestination().parent());
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

            ReindexScriptApplier(WorkingBulkByScrollTask task, ScriptService scriptService, Script script,
                                 Map<String, Object> params) {
                super(task, scriptService, script, params);
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
