/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plugin.ComputeService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Coordinator-local source operator that delegates to the EQL engine: on first poll it issues a single
 * {@code EqlSearchAction} asynchronously, and once the (bounded) response arrives it converts it to one
 * {@link Page} under the {@code EQL} command's fixed schema. The request runs in the caller's security
 * context, so the user's privileges on the EQL target indices apply.
 *
 * <p>The operator is single-shot: it emits at most one page. It never blocks a compute thread — while the
 * search is in flight {@link #isBlocked()} returns a listener the driver waits on, and the async action is
 * tracked on the {@link DriverContext} so the driver is not considered finished prematurely.
 */
public class EqlSourceOperator extends SourceOperator {

    public record Factory(
        Client client,
        @Nullable EqlSearchRequest request,
        Set<String> searchedClusters,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        Source source,
        boolean warnOnDefaultSize,
        EsqlExecutionInfo executionInfo
    ) implements SourceOperatorFactory {
        @Override
        public String describe() {
            String indices = request == null ? "" : String.join(",", request.indices());
            return "EqlSourceOperator[mode=" + mode + ", indices=" + indices + "]";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new EqlSourceOperator(
                driverContext,
                client,
                request,
                searchedClusters,
                mode,
                schema,
                source,
                warnOnDefaultSize,
                executionInfo
            );
        }
    }

    private static final int MAX_REPORTED_FAILURES = 5;
    private static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private final DriverContext driverContext;
    private final Client client;
    private final EqlSearchRequest request;
    private final Set<String> searchedClusters;
    private final EqlRelation.Mode mode;
    private final List<Attribute> schema;
    private final Source source;
    private final boolean warnOnDefaultSize;
    private final EsqlExecutionInfo executionInfo;

    private boolean requested;
    private boolean emitted;
    private SubscribableListener<Void> blocked;
    private volatile Page page;
    private volatile Exception failure;
    // Guards the close()-vs-response-callback race: Driver.drainAndCloseOperators runs before waitForAsyncActions,
    // so close() can fire while the EQL search is still in flight. Mutations of page/closed are synchronized(this).
    private boolean closed;

    private EqlSourceOperator(
        DriverContext driverContext,
        Client client,
        EqlSearchRequest request,
        Set<String> searchedClusters,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        Source source,
        boolean warnOnDefaultSize,
        EsqlExecutionInfo executionInfo
    ) {
        this.driverContext = driverContext;
        this.client = client;
        this.request = request;
        this.searchedClusters = searchedClusters;
        this.mode = mode;
        this.schema = schema;
        this.source = source;
        this.warnOnDefaultSize = warnOnDefaultSize;
        this.executionInfo = executionInfo;
    }

    private void ensureRequested() {
        if (requested) {
            return;
        }
        requested = true;
        blocked = new SubscribableListener<>();
        if (request == null) {
            blocked.onResponse(null);
            return;
        }
        driverContext.addAsyncAction();
        // client.execute delivers every outcome — including a synchronous registration failure such as a
        // TaskCancelledException when the parent task is already banned — through the listener, never by throwing
        // (NodeClient.doExecute forwards those to onFailure), so the async action is always released below.
        client.execute(EqlSearchAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                // Convert synchronously inside the callback and copy the bytes we need out of the (ref-counted)
                // response into blocks. We do not own a reference here — the EQL transport action delivers the
                // response via respondAndRelease and releases it once this listener returns — so we must not
                // decRef it ourselves (doing so over-releases).
                // This callback runs on the EQL response thread, outside the driver run loop, so the blocks must come from
                // the root factory: the driver's local breaker may only be touched by the driver thread.
                // isPartial() only flags a still-running async response; a completed one reports lost shards in shardFailures().
                recordPartialResults(response);
                recordSkippedClusters(response);
                Page built = EqlPageConverter.toPage(response, mode, schema, driverContext.blockFactory().parent());
                synchronized (this) {
                    if (closed) {
                        // The operator was closed (cancellation / sibling-operator failure) before the response
                        // arrived; release the freshly-built page here or its breaker-accounted blocks leak.
                        built.releaseBlocks();
                    } else {
                        page = built;
                    }
                }
            } catch (Exception e) {
                failure = e;
            } finally {
                driverContext.removeAsyncAction();
                blocked.onResponse(null);
            }
        }, e -> {
            try {
                if (skipUnavailableClusters(e) == false) {
                    failure = e;
                }
            } catch (Exception skipFailure) {
                e.addSuppressed(skipFailure);
                failure = e;
            } finally {
                driverContext.removeAsyncAction();
                blocked.onResponse(null);
            }
        }));
        maybeWarnDefaultSize();
    }

    @Override
    public IsBlockedResult isBlocked() {
        ensureRequested();
        return blocked.isDone() ? NOT_BLOCKED : new IsBlockedResult(blocked, "eql");
    }

    @Override
    public Page getOutput() {
        ensureRequested();
        if (blocked.isDone() == false) {
            return null;
        }
        if (failure != null) {
            Exception e = failure;
            failure = null;
            throw ExceptionsHelper.convertToRuntime(e);
        }
        Page result = page;
        page = null;
        emitted = true;
        return result;
    }

    private void maybeWarnDefaultSize() {
        if (warnOnDefaultSize) {
            driverContext.createOnlyWarnings(source)
                .registerWarningWithoutSourceText(
                    "EQL query used the default size of [" + request.size() + "] matches. Set WITH {\"size\": n} to return more."
                );
        }
    }

    private void recordPartialResults(EqlSearchResponse response) {
        if (response.isTimeout()) {
            executionInfo.markPartial();
        }
        ShardSearchFailure[] failures = response.shardFailures();
        if (CollectionUtils.isEmpty(failures)) {
            return;
        }
        executionInfo.markPartial();
        String unnamedCluster = searchedClusters.size() == 1 ? searchedClusters.iterator().next() : LOCAL_CLUSTER;
        Map<String, List<ShardSearchFailure>> failuresByCluster = new HashMap<>();
        for (ShardSearchFailure failure : failures) {
            String cluster = failure.shard() == null || failure.shard().getClusterAlias() == null
                ? unnamedCluster
                : failure.shard().getClusterAlias();
            failuresByCluster.computeIfAbsent(cluster, k -> new ArrayList<>()).add(failure);
        }
        boolean mainPlan = executionInfo.isMainPlan();
        failuresByCluster.forEach((cluster, clusterFailures) -> {
            if (executionInfo.getCluster(cluster) != null) {
                executionInfo.swapCluster(cluster, (alias, current) -> withFailures(current, clusterFailures, mainPlan));
            }
        });
    }

    private static EsqlExecutionInfo.Cluster withFailures(
        EsqlExecutionInfo.Cluster cluster,
        List<ShardSearchFailure> failures,
        boolean mainPlan
    ) {
        Set<String> listed = new HashSet<>();
        for (ShardSearchFailure failure : cluster.getFailures()) {
            listed.add(failedShard(failure));
        }
        List<ShardSearchFailure> added = new ArrayList<>();
        for (ShardSearchFailure failure : failures) {
            if (added.size() < MAX_REPORTED_FAILURES && listed.add(failedShard(failure))) {
                added.add(failure);
            }
        }
        EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(cluster).addFailures(added);
        // The main plan only searches a remote cluster that is still RUNNING, so a subplan leaves a remote cluster's status to it.
        boolean local = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(cluster.getClusterAlias());
        if ((mainPlan || local)
            && (cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING
                || cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)) {
            builder.setStatus(EsqlExecutionInfo.Cluster.Status.PARTIAL);
        }
        return builder.build();
    }

    // EQL lists a shard once per search that lost it, and a FORK runs the search once per branch.
    private static String failedShard(ShardSearchFailure failure) {
        return failure.shard() == null ? failure.reason() : failure.shard().getShardId().toString();
    }

    private void recordSkippedClusters(EqlSearchResponse response) {
        if (response.skippedClusters().isEmpty()) {
            return;
        }
        Set<String> clustersWithEvents = clustersWithEvents(response);
        response.skippedClusters().forEach((cluster, failures) -> {
            if (LOCAL_CLUSTER.equals(cluster)) {
                return;
            }
            executionInfo.markPartial();
            if (executionInfo.getCluster(cluster) != null) {
                List<ShardSearchFailure> listed = failures.subList(0, Math.min(failures.size(), MAX_REPORTED_FAILURES));
                ComputeService.markClusterAfterRuntimeBranchFailure(executionInfo, cluster, clustersWithEvents.contains(cluster), listed);
            }
        });
    }

    // A sequence can hold events a remote cluster returned before a later search skipped it.
    private static Set<String> clustersWithEvents(EqlSearchResponse response) {
        List<EqlSearchResponse.Event> events = new ArrayList<>();
        if (response.hits().events() != null) {
            events.addAll(response.hits().events());
        }
        if (response.hits().sequences() != null) {
            response.hits().sequences().forEach(sequence -> events.addAll(sequence.events()));
        }
        Set<String> clusters = new HashSet<>();
        for (EqlSearchResponse.Event event : events) {
            if (event.missing() == false && event.index() != null) {
                clusters.add(RemoteClusterAware.splitIndexName(event.index()).getClusterGroupingKey());
            }
        }
        return clusters;
    }

    private boolean skipUnavailableClusters(Exception e) {
        // Not through causes: EQL fails a search that lost a shard with the shard's own failure as the cause.
        if (ExceptionsHelper.isTopLevelRemoteUnavailableException(e) == false) {
            return false;
        }
        List<String> remotes = new ArrayList<>();
        for (String cluster : searchedClusters) {
            EsqlExecutionInfo.Cluster info = executionInfo.getCluster(cluster);
            if (LOCAL_CLUSTER.equals(cluster) && info != null && info.getStatus() == EsqlExecutionInfo.Cluster.Status.SKIPPED) {
                continue;
            }
            if (info == null || executionInfo.shouldSkipOnFailure(cluster) == false) {
                return false;
            }
            remotes.add(cluster);
        }
        if (remotes.isEmpty()) {
            return false;
        }
        List<ShardSearchFailure> failures = List.of(new ShardSearchFailure(e));
        for (String cluster : remotes) {
            ComputeService.markClusterAfterRuntimeBranchFailure(executionInfo, cluster, false, failures);
        }
        return true;
    }

    @Override
    public boolean isFinished() {
        // Stay "not finished" while a failure is pending so the driver calls getOutput() and the exception propagates.
        return emitted && failure == null;
    }

    @Override
    public void finish() {
        emitted = true;
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            if (page != null) {
                page.releaseBlocks();
                page = null;
            }
        }
    }
}
