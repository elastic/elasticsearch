/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The coordinator-side dispatcher of the ES|QL federation <b>execution</b> half — the node singleton that packages the
 * dispatch dance {@link ClusterComputeHandler#startComputeOnRemoteCluster} performs for a CCS remote, but for a single
 * remote-abstraction leaf. It is the mirror image of {@link AbstractionComputeHandler}: the handler is the home-cluster
 * <em>receiver</em> of an {@link ExecuteAbstractionRequest}; this service is the querying-cluster <em>sender</em>.
 *
 * <p>Given a per-leaf {@link ExchangeSourceHandler}, {@link #fetchAbstraction} opens an exchange to the home cluster
 * ({@link ExchangeService#openExchange}), sends the name-based request over that connection, wires a single remote sink
 * into the caller's source handler ({@link ExchangeService#newRemoteSink} + {@link ExchangeSourceHandler#addRemoteSink}),
 * and completes the caller's listener when the sink drains. The caller then drains that source handler for pages — exactly
 * the coordinator side of the CCS precedent, but into a leaf-owned handler rather than the plan's single main exchange
 * source (see the engine-question analysis: {@code LocalExecutionPlanner} owns one main exchange source, so a
 * remote-abstraction leaf must own its own).
 *
 * <p><b>Wiring.</b> Constructed in {@code TransportEsqlQueryAction}'s constructor next to {@code EnrichLookupService} /
 * {@code LookupFromIndexService}, passed into {@code ComputeService}, and (Increment 3) into {@code LocalExecutionPlanner}
 * so the yet-to-be-built {@code RemoteAbstractionSourceOperator} can reach it — the identical node-singleton injection
 * path those two lookup services use. Like them it is built <em>before</em> {@code ComputeService} and threaded in, so it
 * carries only node singletons (transport, exchange) and does not depend back on {@code ComputeService}. It is a stateless
 * dispatcher (holds only node singletons + does dispatch), not a {@code Context}: no per-query state lives on it.
 *
 * <p><b>Once-per-leaf dispatch (Inc-1 review S1/S3).</b> {@link #fetchAbstraction} performs exactly one
 * {@code openExchange} + one {@code sendChildRequest} + one {@code addRemoteSink} per call, and it takes the leaf's
 * {@link ExchangeSourceHandler} as a parameter rather than creating it. The contract for Increment 3's operator is
 * therefore: the leaf's factory creates one {@link ExchangeSourceHandler} and calls {@link #fetchAbstraction} once —
 * lazily, at the leaf level — and every parallel operator instance ({@code DriverParallelism > 1}) only
 * {@link ExchangeSourceHandler#createExchangeSource}s from that already-dispatched handler. The dispatch is a per-leaf
 * action, not per-operator-instance; this service holds no state that could make it otherwise. A second call with a
 * fresh session id would be a second full remote execution — the API makes the single call the leaf's responsibility,
 * exactly as CCS makes {@code startComputeOnRemoteCluster} the per-cluster responsibility.
 */
public class FederationExecutionService {

    private final TransportService transportService;
    private final ExchangeService exchangeService;
    private final RemoteClusterService remoteClusterService;
    private final Executor searchExecutor;
    // Per-leaf child-session id source, mirroring ComputeService.newChildSession: a monotonic suffix off the query
    // session id so each leaf's exchange is distinct. Owned here (not delegated to ComputeService) so this service does
    // not depend back on ComputeService — it is constructed before it, exactly like the lookup services.
    private final AtomicLong childSessionIdGenerator = new AtomicLong();

    FederationExecutionService(TransportService transportService, ExchangeService exchangeService, Executor searchExecutor) {
        this.transportService = transportService;
        this.exchangeService = exchangeService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.searchExecutor = searchExecutor;
    }

    /**
     * Dispatches one remote-abstraction leaf: opens an exchange to {@code handle}'s home cluster, sends an
     * {@link ExecuteAbstractionRequest} carrying only {@code abstractionName} + {@code expectedAttributes}, and adds a
     * single remote sink into {@code leafExchangeSource}. When the sink finishes draining, {@code completion} completes;
     * the caller drives {@code leafExchangeSource.createExchangeSource()} to read the pages. Mirrors
     * {@link ClusterComputeHandler#startComputeOnRemoteCluster}: {@code openExchange} → {@code sendChildRequest} →
     * {@code newRemoteSink} → {@code addRemoteSink}.
     *
     * <p>Federation execution fails fast: a home-side failure finishes the sink with the failure and completes
     * {@code completion} exceptionally, so the leaf drain terminates loud rather than silently returning fewer rows
     * (partial/skip-unavailable parity with CCS is a follow-up — see the Inc-1 review's S4).
     *
     * @param sessionId          the coordinator's query session id; a fresh child session id is derived from it so this
     *                           leaf's exchange never collides with the plan's main exchange or a sibling leaf
     * @param parentTask         the query's root task; the child request and remote sink are parented to it so
     *                           cancellation propagates to the home cluster
     * @param configuration      the coordinator configuration, carried in the request (today only the
     *                           {@code PlanStreamOutput} context for the expected-schema attributes; timezone/locale
     *                           propagation is a follow-up)
     * @param handle             the cluster alias of the abstraction's home cluster, or the empty string for a
     *                           local/same-cluster abstraction (resolved to the local node connection)
     * @param abstractionName    the view/dataset name to resolve and run on the home cluster
     * @param expectedAttributes the ordered output schema the coordinator resolved against; the home handler validates
     *                           its fresh resolution against this and fails loud on drift (B1 schema-drift guard)
     * @param leafExchangeSource the caller-owned per-leaf source handler the remote sink is added to; the caller drains it
     * @param completion         completes when the remote sink finishes draining, or exceptionally on any failure
     */
    public void fetchAbstraction(
        String sessionId,
        CancellableTask parentTask,
        Configuration configuration,
        String handle,
        String abstractionName,
        List<Attribute> expectedAttributes,
        ExchangeSourceHandler leafExchangeSource,
        ActionListener<Void> completion
    ) {
        final Transport.Connection connection;
        try {
            connection = connectionFor(handle);
        } catch (Exception e) {
            completion.onFailure(e);
            return;
        }

        // Refuse to dispatch to a home cluster that predates the execute_abstraction action rather than letting the send
        // hit an unknown-action rejection: the whole request type is new wire, so an older remote has no handler for it.
        // (Companion to the version-gate-planner-rewrites discipline: never send a newer wire type to an older remote.)
        if (connection.getTransportVersion().supports(ExecuteAbstractionRequest.ESQL_EXECUTE_ABSTRACTION) == false) {
            completion.onFailure(
                new IllegalStateException(
                    "cannot execute abstraction ["
                        + abstractionName
                        + "] on cluster ["
                        + clusterLabel(handle)
                        + "]: it is on an older version that does not support remote abstraction execution"
                )
            );
            return;
        }

        final int bufferSize = configuration.pragmas().exchangeBufferSize();
        final String childSessionId = sessionId + "/" + childSessionIdGenerator.incrementAndGet();
        final ExecuteAbstractionRequest request = new ExecuteAbstractionRequest(
            handle,
            childSessionId,
            configuration,
            abstractionName,
            expectedAttributes
        );

        // openExchange creates the sink handler on the home cluster under childSessionId; on success, send the request so
        // the home cluster resolves+runs the name and sinks pages into that handler, then wire a single remote sink into
        // the caller's leaf source so the coordinator can drain them. Mirror of startComputeOnRemoteCluster's ordering.
        ExchangeService.openExchange(
            transportService,
            connection,
            childSessionId,
            bufferSize,
            searchExecutor,
            completion.delegateFailureAndWrap((delegate, unused) -> {
                transportService.sendChildRequest(
                    connection,
                    AbstractionComputeHandler.EXECUTE_ABSTRACTION_ACTION_NAME,
                    request,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        // The ComputeResponse (took + shard counts) is metadata; correctness of the leaf is proven by the
                        // drained pages + the sink completion below. Discard the response body but propagate its failure.
                        ActionListener.noop(),
                        ComputeResponse::new,
                        searchExecutor
                    )
                );
                var remoteSink = exchangeService.newRemoteSink(parentTask, childSessionId, transportService, connection);
                // failFast=true: federation execution surfaces a home-side failure rather than going silently partial.
                // concurrentExchangeClients=1: one home cluster, one sink — the leaf is a single remote source.
                leafExchangeSource.addRemoteSink(remoteSink, true, () -> {}, 1, delegate);
            })
        );
    }

    /**
     * The connection to {@code handle}'s home cluster. A remote handle resolves through {@link RemoteClusterService}; the
     * empty handle is a local/same-cluster abstraction, which uses the local node connection (the same connection the
     * Inc-1 IT dispatches over) — the one place the local case genuinely differs from the cross-cluster one.
     */
    private Transport.Connection connectionFor(String handle) {
        if (isLocalHandle(handle)) {
            return transportService.getLocalNodeConnection();
        }
        return remoteClusterService.getConnection(handle);
    }

    private static boolean isLocalHandle(String handle) {
        return handle == null || RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(handle);
    }

    private static String clusterLabel(String handle) {
        return isLocalHandle(handle) ? "local" : handle;
    }
}
