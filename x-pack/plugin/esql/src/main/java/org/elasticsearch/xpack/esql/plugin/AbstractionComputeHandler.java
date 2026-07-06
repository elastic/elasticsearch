/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * The home-cluster receiver of the ES|QL federation <b>execution</b> half — the sibling of {@link ClusterComputeHandler}
 * on the remote (name-resolving) side. Where {@code ClusterComputeHandler.runComputeOnRemoteCluster} receives a physical
 * plan fragment and reduces it, this handler receives only an abstraction's <b>name</b>
 * ({@link ExecuteAbstractionRequest}) and resolves it through the home cluster's OWN kind-blind {@code SchemaService}
 * umbrella ({@code resolvePlan}, driven via {@link org.elasticsearch.xpack.esql.execution.PlanExecutor#esql}), plans the
 * resolved body, runs it through the existing compute path, and sinks the result pages into the exchange sink identified
 * by {@link ExecuteAbstractionRequest#sessionId()}. The coordinator on the querying cluster opened that sink via
 * {@link ExchangeService#openExchange} and polls pages from it.
 *
 * <p>The exchange lifecycle (get-sink-handler, cancel/complete finish, sink supplier) mirrors
 * {@code ClusterComputeHandler.runComputeOnRemoteCluster} exactly. The genuinely new capability is remote-side planning
 * of a <em>named</em> abstraction: the handler does not receive a plan, it re-derives one from the name.
 *
 * <p><b>Schema-drift guard (B1).</b> Because the name is re-resolved on the home cluster at a different time than the
 * coordinator resolved its schema, the handler validates the freshly-resolved plan's output against
 * {@link ExecuteAbstractionRequest#expectedAttributes()} (same names, types, order) before sinking any page. Exchange
 * pages carry no schema header, so without this check a redefined view / drifted dataset would stream column-swapped
 * pages positionally into the coordinator's layout — a silent wrong answer. On mismatch the handler fails loud.
 */
final class AbstractionComputeHandler implements TransportRequestHandler<ExecuteAbstractionRequest> {

    /** {@code "indices:data/read/esql/execute_abstraction"} — {@code indices:}-scoped, sibling of {@code .../cluster}. */
    static final String EXECUTE_ABSTRACTION_ACTION_NAME = EsqlQueryAction.NAME + "/execute_abstraction";

    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final TransportService transportService;
    private final AbstractionResolver abstractionResolver;

    /**
     * The home-cluster resolve+plan seam. The transport action supplies this so the full {@code planExecutor.esql(...)}
     * argument assembly (resolvers, services, external-source executor, live analyzer settings) lives in one place — the
     * same place {@code innerExecute} assembles it — rather than being duplicated here. It synthesizes a
     * {@code FROM <name>} request, builds the per-request {@link EsqlExecutionInfo}, and drives
     * {@code planExecutor.esql(...)} with a sink-bound {@code PlanRunner} produced by {@code runnerFactory} — the factory
     * takes the freshly-built execution info so the runner's {@code executePlan} threads the same info the session uses.
     */
    interface AbstractionResolver {
        void resolveAndExecute(
            String abstractionName,
            CancellableTask parentTask,
            Function<EsqlExecutionInfo, EsqlSession.PlanRunner> runnerFactory,
            ActionListener<Result> listener
        );
    }

    AbstractionComputeHandler(
        ComputeService computeService,
        ExchangeService exchangeService,
        TransportService transportService,
        Executor searchExecutor,
        AbstractionResolver abstractionResolver
    ) {
        this.computeService = computeService;
        this.exchangeService = exchangeService;
        this.transportService = transportService;
        this.abstractionResolver = abstractionResolver;
        transportService.registerRequestHandler(EXECUTE_ABSTRACTION_ACTION_NAME, searchExecutor, ExecuteAbstractionRequest::new, this);
    }

    @Override
    public void messageReceived(ExecuteAbstractionRequest request, TransportChannel channel, Task task) {
        ChannelActionListener<ComputeResponse> listener = new ChannelActionListener<>(channel);
        runAbstractionOnHomeCluster(request, (CancellableTask) task, listener);
    }

    /**
     * Resolves {@code request.abstractionName()} through the home cluster's umbrella, plans + runs the body, and sinks
     * the result pages into the exchange sink at {@code request.sessionId()}. Cancel/complete finish-handler wiring
     * mirrors {@code ClusterComputeHandler.runComputeOnRemoteCluster}.
     */
    void runAbstractionOnHomeCluster(
        ExecuteAbstractionRequest request,
        CancellableTask parentTask,
        ActionListener<ComputeResponse> listener
    ) {
        final String sessionId = request.sessionId();
        final var exchangeSink = exchangeService.getSinkHandler(sessionId);
        parentTask.addListener(
            () -> exchangeService.finishSinkHandler(sessionId, new TaskCancelledException(parentTask.getReasonCancelled()))
        );
        exchangeSink.addCompletionListener(ActionListener.running(() -> exchangeService.finishSinkHandler(sessionId, null)));

        // Sink-bound PlanRunner factory: instead of collecting pages (computeService.execute), sink them into the
        // exchange the coordinator polls (computeService.executePlan with a non-null sink supplier — the same path the
        // subquery executor already uses). The resolver hands us the per-request EsqlExecutionInfo so executePlan
        // threads the same info the session built. Validate the resolved schema against the coordinator's expectation
        // (B1) before running.
        Function<EsqlExecutionInfo, EsqlSession.PlanRunner> runnerFactory = execInfo -> (
            plan,
            configuration,
            foldCtx,
            planTimeProfile,
            resultListener) -> {
            try {
                validateSchema(request.abstractionName(), request.expectedAttributes(), plan);
            } catch (Exception e) {
                resultListener.onFailure(e);
                return;
            }
            computeService.executePlan(
                sessionId,
                parentTask,
                computeService.createFlags(),
                plan,
                configuration,
                foldCtx,
                execInfo,
                ComputeService.DATA_DESCRIPTION,
                resultListener,
                () -> exchangeSink.createExchangeSink(() -> {}),
                Map.of(),
                planTimeProfile
            );
        };

        abstractionResolver.resolveAndExecute(
            request.abstractionName(),
            parentTask,
            runnerFactory,
            listener.map(result -> new ComputeResponse(result.completionInfo()))
        );
    }

    /**
     * Fails loud if the freshly-resolved plan's output does not match the coordinator's expected schema by name, type,
     * and order. This is the schema-drift guard (B1): name-based execution re-resolves the abstraction on the home
     * cluster, and exchange pages are positional with no schema header — a mismatch here would silently mis-bind columns.
     */
    private static void validateSchema(String abstractionName, List<Attribute> expected, PhysicalPlan plan) {
        List<Attribute> actual = plan.output();
        boolean matches = actual.size() == expected.size();
        for (int i = 0; matches && i < actual.size(); i++) {
            Attribute a = actual.get(i);
            Attribute e = expected.get(i);
            DataType at = a.dataType();
            DataType et = e.dataType();
            if (a.name().equals(e.name()) == false || at != et) {
                matches = false;
            }
        }
        if (matches == false) {
            throw new IllegalStateException(
                "schema drift executing abstraction ["
                    + abstractionName
                    + "]: coordinator resolved "
                    + describe(expected)
                    + " but home cluster resolved "
                    + describe(actual)
            );
        }
    }

    private static String describe(List<Attribute> attributes) {
        return attributes.stream().map(a -> a.name() + ":" + a.dataType().typeName()).toList().toString();
    }
}
