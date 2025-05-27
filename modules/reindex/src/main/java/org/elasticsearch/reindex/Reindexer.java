/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.WorkerBulkByScrollTaskState;
import org.elasticsearch.reindex.remote.RemoteScrollableHitSource;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.ReindexMetadata;
import org.elasticsearch.script.ReindexScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedList;
import static org.elasticsearch.index.VersionType.INTERNAL;

public class Reindexer {

    private static final Logger logger = LogManager.getLogger(Reindexer.class);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final Client client;
    private final ThreadPool threadPool;
    private final ScriptService scriptService;
    private final ReindexSslConfig reindexSslConfig;
    private final ReindexMetrics reindexMetrics;

    Reindexer(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Client client,
        ThreadPool threadPool,
        ScriptService scriptService,
        ReindexSslConfig reindexSslConfig,
        @Nullable ReindexMetrics reindexMetrics
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.client = client;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.reindexSslConfig = reindexSslConfig;
        this.reindexMetrics = reindexMetrics;
    }

    public void initTask(BulkByScrollTask task, ReindexRequest request, ActionListener<Void> listener) {
        BulkByScrollParallelizationHelper.initTaskState(task, request, client, listener);
    }

    public void execute(BulkByScrollTask task, ReindexRequest request, Client bulkClient, ActionListener<BulkByScrollResponse> listener) {
        long startTime = System.nanoTime();

        BulkByScrollParallelizationHelper.executeSlicedAction(
            task,
            request,
            ReindexAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            () -> {
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(client, clusterService.localNode(), task);
                ParentTaskAssigningClient assigningBulkClient = new ParentTaskAssigningClient(bulkClient, clusterService.localNode(), task);
                AsyncIndexBySearchAction searchAction = new AsyncIndexBySearchAction(
                    task,
                    logger,
                    assigningClient,
                    assigningBulkClient,
                    threadPool,
                    scriptService,
                    projectResolver.getProjectState(clusterService.state()),
                    reindexSslConfig,
                    request,
                    ActionListener.runAfter(listener, () -> {
                        long elapsedTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                        if (reindexMetrics != null) {
                            reindexMetrics.recordTookTime(elapsedTime);
                        }
                    })
                );
                searchAction.start();
            }
        );
    }

    /**
     * Build the {@link RestClient} used for reindexing from remote clusters.
     * @param remoteInfo connection information for the remote cluster
     * @param sslConfig configuration for potential outgoing HTTPS connections
     * @param taskId the id of the current task. This is added to the thread name for easier tracking
     * @param threadCollector a list in which we collect all the threads created by the client
     */
    static RestClient buildRestClient(RemoteInfo remoteInfo, ReindexSslConfig sslConfig, long taskId, List<Thread> threadCollector) {
        Header[] clientHeaders = new Header[remoteInfo.getHeaders().size()];
        int i = 0;
        for (Map.Entry<String, String> header : remoteInfo.getHeaders().entrySet()) {
            clientHeaders[i++] = new BasicHeader(header.getKey(), header.getValue());
        }
        final RestClientBuilder builder = RestClient.builder(
            new HttpHost(remoteInfo.getHost(), remoteInfo.getPort(), remoteInfo.getScheme())
        ).setDefaultHeaders(clientHeaders).setRequestConfigCallback(c -> {
            c.setConnectTimeout(Math.toIntExact(remoteInfo.getConnectTimeout().millis()));
            c.setSocketTimeout(Math.toIntExact(remoteInfo.getSocketTimeout().millis()));
            return c;
        }).setHttpClientConfigCallback(c -> {
            // Enable basic auth if it is configured
            if (remoteInfo.getUsername() != null) {
                UsernamePasswordCredentials creds = new UsernamePasswordCredentials(
                    remoteInfo.getUsername(),
                    remoteInfo.getPassword().toString()
                );
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
            c.setSSLStrategy(sslConfig.getStrategy());
            return c;
        });
        if (Strings.hasLength(remoteInfo.getPathPrefix()) && "/".equals(remoteInfo.getPathPrefix()) == false) {
            builder.setPathPrefix(remoteInfo.getPathPrefix());
        }
        return builder.build();
    }

    /**
     * Simple implementation of reindex using scrolling and bulk. There are tons
     * of optimizations that can be done on certain types of reindex requests
     * but this makes no attempt to do any of them so it can be as simple
     * possible.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByScrollAction<ReindexRequest, TransportReindexAction> {
        /**
         * Mapper for the {@code _id} of the destination index used to
         * normalize {@code _id}s landing in the index.
         */
        private final IdFieldMapper destinationIndexIdMapper;

        /**
         * List of threads created by this process. Usually actions don't create threads in Elasticsearch. Instead they use the builtin
         * {@link ThreadPool}s. But reindex-from-remote uses Elasticsearch's {@link RestClient} which doesn't use the
         * {@linkplain ThreadPool}s because it uses httpasyncclient. It'd be a ton of trouble to work around creating those threads. So
         * instead we let it create threads but we watch them carefully and assert that they are dead when the process is over.
         */
        private List<Thread> createdThreads = emptyList();

        AsyncIndexBySearchAction(
            BulkByScrollTask task,
            Logger logger,
            ParentTaskAssigningClient searchClient,
            ParentTaskAssigningClient bulkClient,
            ThreadPool threadPool,
            ScriptService scriptService,
            ProjectState state,
            ReindexSslConfig sslConfig,
            ReindexRequest request,
            ActionListener<BulkByScrollResponse> listener
        ) {
            super(
                task,
                /*
                 * We only need the source version if we're going to use it when write and we only do that when the destination request uses
                 * external versioning.
                 */
                request.getDestination().versionType() != VersionType.INTERNAL,
                false,
                logger,
                searchClient,
                bulkClient,
                threadPool,
                request,
                listener,
                scriptService,
                sslConfig
            );
            this.destinationIndexIdMapper = destinationIndexMode(state).idFieldMapperWithoutFieldData();
        }

        private IndexMode destinationIndexMode(ProjectState state) {
            ProjectMetadata projectMetadata = state.metadata();
            IndexMetadata destMeta = projectMetadata.index(mainRequest.getDestination().index());
            if (destMeta != null) {
                return IndexSettings.MODE.get(destMeta.getSettings());
            }
            String template = MetadataIndexTemplateService.findV2Template(projectMetadata, mainRequest.getDestination().index(), false);
            if (template == null) {
                return IndexMode.STANDARD;
            }
            Settings settings = MetadataIndexTemplateService.resolveSettings(projectMetadata, template);
            // We retrieve the setting without performing any validation because that the template has already been validated
            String indexMode = settings.get(IndexSettings.MODE.getKey());
            return indexMode == null ? IndexMode.STANDARD : IndexMode.fromString(indexMode);
        }

        @Override
        protected ScrollableHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy, SearchRequest searchRequest) {
            if (mainRequest.getRemoteInfo() != null) {
                RemoteInfo remoteInfo = mainRequest.getRemoteInfo();
                createdThreads = synchronizedList(new ArrayList<>());
                assert sslConfig != null : "Reindex ssl config must be set";
                RestClient restClient = buildRestClient(remoteInfo, sslConfig, task.getId(), createdThreads);
                return new RemoteScrollableHitSource(
                    logger,
                    backoffPolicy,
                    threadPool,
                    worker::countSearchRetry,
                    this::onScrollResponse,
                    this::finishHim,
                    restClient,
                    remoteInfo,
                    searchRequest
                );
            }
            return super.buildScrollableResultSource(backoffPolicy, searchRequest);
        }

        @Override
        protected void finishHim(
            Exception failure,
            List<BulkItemResponse.Failure> indexingFailures,
            List<ScrollableHitSource.SearchFailure> searchFailures,
            boolean timedOut
        ) {
            super.finishHim(failure, indexingFailures, searchFailures, timedOut);
            // A little extra paranoia so we log something if we leave any threads running
            for (Thread thread : createdThreads) {
                if (thread.isAlive()) {
                    assert false : "Failed to properly stop client thread [" + thread.getName() + "]";
                    logger.error("Failed to properly stop client thread [{}]", thread.getName());
                }
            }
        }

        @Override
        public BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                assert scriptService != null : "Script service must be set";
                return new ReindexScriptApplier(worker, scriptService, script, script.getParams(), threadPool::absoluteTimeInMillis);
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(ScrollableHitSource.Hit doc) {
            IndexRequest index = new IndexRequest();

            // Copy the index from the request so we always write where it asked to write
            index.index(mainRequest.getDestination().index());

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
            index.id(destinationIndexIdMapper.reindexId(doc.getId()));

            // the source xcontent type and destination could be different
            final XContentType sourceXContentType = doc.getXContentType();
            final XContentType mainRequestXContentType = mainRequest.getDestination().getContentType();
            if (mainRequestXContentType != null && doc.getXContentType() != mainRequestXContentType) {
                // we need to convert
                try (
                    XContentParser parser = XContentHelper.createParserNotCompressed(
                        XContentParserConfiguration.EMPTY,
                        doc.getSource(),
                        sourceXContentType
                    );
                    XContentBuilder builder = XContentBuilder.builder(mainRequestXContentType.xContent())
                ) {
                    parser.nextToken();
                    builder.copyCurrentStructure(parser);
                    index.source(BytesReference.bytes(builder), builder.contentType());
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "failed to convert hit from " + sourceXContentType + " to " + mainRequestXContentType,
                        e
                    );
                }
            } else {
                index.source(doc.getSource(), doc.getXContentType());
            }

            /*
             * The rest of the index request just has to be copied from the template. It may be changed later from scripts or the superclass
             * here on out operates on the index request rather than the template.
             */
            index.routing(mainRequest.getDestination().routing());
            index.setPipeline(mainRequest.getDestination().getPipeline());
            if (mainRequest.getDestination().opType() == DocWriteRequest.OpType.CREATE) {
                index.opType(mainRequest.getDestination().opType());
            }

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
                case "keep" -> super.copyRouting(request, routing);
                case "discard" -> super.copyRouting(request, null);
                default -> throw new IllegalArgumentException("Unsupported routing command");
            }
        }

        static class ReindexScriptApplier extends ScriptApplier<ReindexMetadata> {
            private ReindexScript.Factory reindex;

            ReindexScriptApplier(
                WorkerBulkByScrollTaskState taskWorker,
                ScriptService scriptService,
                Script script,
                Map<String, Object> params,
                LongSupplier nowInMillisSupplier
            ) {
                super(taskWorker, scriptService, script, params, nowInMillisSupplier);
            }

            @Override
            protected CtxMap<ReindexMetadata> execute(ScrollableHitSource.Hit doc, Map<String, Object> source) {
                if (reindex == null) {
                    reindex = scriptService.compile(script, ReindexScript.CONTEXT);
                }
                CtxMap<ReindexMetadata> ctxMap = new CtxMap<>(
                    source,
                    new ReindexMetadata(
                        doc.getIndex(),
                        doc.getId(),
                        doc.getVersion(),
                        doc.getRouting(),
                        INDEX,
                        nowInMillisSupplier.getAsLong()
                    )
                );
                reindex.newInstance(params, ctxMap).execute();
                return ctxMap;
            }

            @Override
            protected void updateRequest(RequestWrapper<?> request, ReindexMetadata metadata) {
                if (metadata.indexChanged()) {
                    request.setIndex(metadata.getIndex());
                }
                if (metadata.idChanged()) {
                    request.setId(metadata.getId());
                }
                if (metadata.versionChanged()) {
                    if (metadata.isVersionInternal()) {
                        request.setVersion(Versions.MATCH_ANY);
                        request.setVersionType(INTERNAL);
                    } else {
                        request.setVersion(metadata.getVersion());
                    }
                }
                /*
                 * Its important that routing comes after parent in case you want to
                 * change them both.
                 */
                if (metadata.routingChanged()) {
                    request.setRouting(metadata.getRouting());
                }
            }
        }
    }
}
