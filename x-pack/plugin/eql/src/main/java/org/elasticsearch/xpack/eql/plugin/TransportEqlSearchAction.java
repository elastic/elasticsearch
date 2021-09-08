/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.eql.util.RemoteClusterRegistry;
import org.elasticsearch.xpack.ql.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.ql.expression.Order;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.ql.plugin.TransportActionUtils.executeRequestWithRetryAttempt;

public class TransportEqlSearchAction extends HandledTransportAction<EqlSearchRequest, EqlSearchResponse>
    implements AsyncTaskManagementService.AsyncOperation<EqlSearchRequest, EqlSearchResponse, EqlSearchTask> {

    private static final Logger log = LogManager.getLogger(TransportEqlSearchAction.class);
    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final AsyncTaskManagementService<EqlSearchRequest, EqlSearchResponse, EqlSearchTask> asyncTaskManagementService;

    @Inject
    public TransportEqlSearchAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                    ThreadPool threadPool, ActionFilters actionFilters, PlanExecutor planExecutor,
                                    NamedWriteableRegistry registry, Client client, BigArrays bigArrays) {
        super(EqlSearchAction.NAME, transportService, actionFilters, EqlSearchRequest::new);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
        this.threadPool = threadPool;
        this.transportService = transportService;

        this.asyncTaskManagementService = new AsyncTaskManagementService<>(XPackPlugin.ASYNC_RESULTS_INDEX, client, ASYNC_SEARCH_ORIGIN,
            registry, taskManager, EqlSearchAction.INSTANCE.name(), this, EqlSearchTask.class, clusterService, threadPool, bigArrays);
    }

    @Override
    public EqlSearchTask createTask(EqlSearchRequest request, long id, String type, String action, TaskId parentTaskId,
                                    Map<String, String> headers, Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId) {
        return new EqlSearchTask(id, type, action, request.getDescription(), parentTaskId, headers, originHeaders, asyncExecutionId,
            request.keepAlive());
    }

    @Override
    public void execute(EqlSearchRequest request, EqlSearchTask task, ActionListener<EqlSearchResponse> listener) {
        operation(planExecutor, task, request, username(securityContext), transportService, clusterService, listener);
    }

    @Override
    public EqlSearchResponse initialResponse(EqlSearchTask task) {
        return new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY,
            TimeValue.nsecToMSec(System.nanoTime() - task.getStartTimeNanos()), false, task.getExecutionId().getEncoded(), true, true);
    }

    @Override
    public EqlSearchResponse readResponse(StreamInput inputStream) throws IOException {
        return new EqlSearchResponse(inputStream);
    }

    @Override
    protected void doExecute(Task task, EqlSearchRequest request, ActionListener<EqlSearchResponse> listener) {
        if (requestIsAsync(request)) {
            asyncTaskManagementService.asyncExecute(request, request.waitForCompletionTimeout(), request.keepAlive(),
                request.keepOnCompletion(), listener);
        } else {
            operation(planExecutor, (EqlSearchTask) task, request, username(securityContext), transportService, clusterService, listener);
        }
    }

    public static void operation(PlanExecutor planExecutor, EqlSearchTask task, EqlSearchRequest request, String username,
                                 TransportService transportService, ClusterService clusterService,
                                 ActionListener<EqlSearchResponse> listener) {
        String nodeId = clusterService.localNode().getId();
        String clusterName = clusterName(clusterService);
        // TODO: these should be sent by the client
        ZoneId zoneId = DateUtils.of("Z");
        QueryBuilder filter = request.filter();
        List<FieldAndFormat> fetchFields = request.fetchFields();
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        String clientId = null;

        RemoteClusterRegistry remoteClusterRegistry = new RemoteClusterRegistry(transportService.getRemoteClusterService(),
            request.indicesOptions());
        Set<String> clusterAliases = remoteClusterRegistry.clusterAliases(request.indices(), false);
        if (canMinimizeRountrips(request, clusterAliases)) {
            String clusterAlias = clusterAliases.iterator().next();
            String[] remoteIndices = new String[request.indices().length];
            for (int i = 0; i < request.indices().length; i++) {
                remoteIndices[i] = request.indices()[i].substring(clusterAlias.length() + 1); // strip cluster plus `:` delimiter
            }
            transportService.sendRequest(transportService.getRemoteClusterService().getConnection(clusterAlias),
                EqlSearchAction.INSTANCE.name(), request.indices(remoteIndices), TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(wrap(r -> listener.onResponse(qualifyHits(r, clusterAlias)),
                    e -> listener.onFailure(qualifyException(e, remoteIndices, clusterAlias))),
                    EqlSearchAction.INSTANCE.getResponseReader()));
        } else {
            ParserParams params = new ParserParams(zoneId)
                .fieldEventCategory(request.eventCategoryField())
                .fieldTimestamp(request.timestampField())
                .fieldTiebreaker(request.tiebreakerField())
                .resultPosition("tail".equals(request.resultPosition()) ? Order.OrderDirection.DESC : Order.OrderDirection.ASC)
                .size(request.size())
                .fetchSize(request.fetchSize());

            EqlConfiguration cfg = new EqlConfiguration(request.indices(), zoneId, username, clusterName, filter,
                request.runtimeMappings(), fetchFields, timeout, request.indicesOptions(), request.fetchSize(),
                clientId, new TaskId(nodeId, task.getId()), task, remoteClusterRegistry::versionIncompatibleClusters);
            executeRequestWithRetryAttempt(clusterService, listener::onFailure,
                onFailure -> planExecutor.eql(cfg, request.query(), params,
                    wrap(r -> listener.onResponse(createResponse(r, task.getExecutionId())), onFailure)),
                node -> transportService.sendRequest(node, EqlSearchAction.NAME, request,
                    new ActionListenerResponseHandler<>(listener, EqlSearchResponse::new, ThreadPool.Names.SAME)),
                log);
        }
    }

    static EqlSearchResponse createResponse(Results results, AsyncExecutionId id) {
        EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(results.events(), results.sequences(), results.totalHits());
        if (id != null) {
            return new EqlSearchResponse(hits, results.tookTime().getMillis(), results.timedOut(), id.getEncoded(), false, false);
        } else {
            return new EqlSearchResponse(hits, results.tookTime().getMillis(), results.timedOut());
        }
    }

    private static boolean requestIsAsync(EqlSearchRequest request) {
        return request.waitForCompletionTimeout() != null && request.waitForCompletionTimeout().getMillis() >= 0;
    }

    // can the request be proxied to the remote cluster?
    private static boolean canMinimizeRountrips(EqlSearchRequest request, Set<String> clusterAliases) {
        // Has minimizing the round trips been (explicitly) disabled?
        if (request.ccsMinimizeRoundtrips() == false) {
            return false;
        }
        // Is this a search against a single, remote cluster?
        if (clusterAliases.size() != 1 || clusterAliases.contains(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
            return false;
        }
        // The remote async ID would not be valid on local cluster; furthermore on results fetching we would know neither if the ID is
        // remote or not, nor which remote cluster it belongs to (TODO: could rewrite the ID to smth like [alias:ID])
        return requestIsAsync(request) == false;
    }

    // fixes the _index values by prefixing them with the source cluster alias' name
    private static EqlSearchResponse qualifyHits(EqlSearchResponse r, String clusterAlias) {
        EqlSearchResponse.Hits hits = r.hits();
        if (hits.sequences() != null) {
            for (EqlSearchResponse.Sequence s : hits.sequences()) {
                qualifyEvents(s.events(), clusterAlias);
            }
        } else {
            qualifyEvents(hits.events(), clusterAlias);
        }
        return r;
    }

    private static void qualifyEvents(List<EqlSearchResponse.Event> events, String clusterAlias) {
        if (events != null) {
            for (EqlSearchResponse.Event e : events) {
                e.index(buildRemoteIndexName(clusterAlias, e.index()));
            }
        }
    }

    private static Exception qualifyException(Exception e, String[] indices, String clusterAlias) {
        Exception finalException = e;
        if (e instanceof RemoteTransportException && e.getCause() instanceof IndexNotFoundException) {
            IndexNotFoundException infe = (IndexNotFoundException) e.getCause();
            if (infe.getIndex() != null) {
                String qualifiedIndex;
                String exceptionIndexName = infe.getIndex().getName();
                String[] notFoundIndices = notFoundIndices(exceptionIndexName, indices);
                if (notFoundIndices != null) {
                    StringJoiner sj = new StringJoiner(",");
                    for (String notFoundIndex : notFoundIndices) {
                        sj.add(buildRemoteIndexName(clusterAlias, notFoundIndex));
                    }
                    qualifiedIndex = sj.toString();
                } else {
                    qualifiedIndex = buildRemoteIndexName(clusterAlias, exceptionIndexName);
                }
                // This will expose a "uniform" failure root_cause, with same "type" ("index_not_found_exception") and "reason" ("no such
                // index [...]"); this is also similar to a non-CCS `POST inexistent/_eql/search?ignore_unavailable=false`, but
                // unfortunately unlike an inexistent pattern search: `POST inexistent*/_eql/search?ignore_unavailable=false, which raises a
                // VerificationException as it's root cause. I.e. the failures are not homogenous.
                finalException = new RemoteTransportException(e.getMessage(), new IndexNotFoundException(qualifiedIndex));
            }
        }
        return finalException;
    }

    private static String[] notFoundIndices(String exceptionIndexName, String[] indices) {
        final String[] EXCEPTION_PREFIXES = new String[] {"Unknown index [", "["};
        for (String prefix : EXCEPTION_PREFIXES) {
            if (exceptionIndexName.startsWith(prefix) && exceptionIndexName.endsWith("]")) {
                String indexList = exceptionIndexName.substring(prefix.length(), exceptionIndexName.length() - 1);
                // see RestEqlSearchAction#prepareRequest() or GH#63529 for an explanation of "*,-*" replacement
                return indexList.equals("*,-*") ? indices : indexList.split(",[ ]?");
            }
        }
        return null;
    }

    static String username(SecurityContext securityContext) {
        return securityContext != null && securityContext.getUser() != null ? securityContext.getUser().principal() : null;
    }

    static String clusterName(ClusterService clusterService) {
        return clusterService.getClusterName().value();
    }
}
