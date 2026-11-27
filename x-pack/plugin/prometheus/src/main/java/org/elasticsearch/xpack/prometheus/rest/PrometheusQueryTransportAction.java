/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.ExemplarsQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.InstantQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.LabelValuesQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.LabelsQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.MetadataQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.RangeQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQuery.SeriesQuery;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryResponseListener.QueryMode;
import org.elasticsearch.xpack.prometheus.rest.PromqlQueryPlanBuilder.PromqlStatementResult;

/**
 * Runs the query of a Prometheus read endpoint ({@code query}, {@code query_range}, {@code query_exemplars}, {@code series},
 * {@code labels}, {@code label/{name}/values} and {@code metadata}) as an ES|QL query.
 * <p>
 * Building the ES|QL statement parses the PromQL query or selectors of the request, which can be costly, so it only happens
 * once the request is authorized, and not on a network thread: like an ES|QL query, the request is authorized by this action's
 * name, which the {@code read} index privilege grants, before the action forks to the {@code search} thread pool to build the
 * statement. The statement then runs as an ES|QL query of the same user, which authorizes it as any other, as a child of this
 * action's task, so that cancelling this task, for example when the HTTP client disconnects, cancels the query.
 */
public class PrometheusQueryTransportAction extends HandledTransportAction<PrometheusQueryRequest, PrometheusQueryResponse> {

    public static final String NAME = "indices:data/read/prometheus/query";
    public static final ActionType<PrometheusQueryResponse> TYPE = new ActionType<>(NAME);

    private final TransportService transportService;
    private final Client client;

    @Inject
    public PrometheusQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(NAME, transportService, actionFilters, in -> TransportAction.localOnly(), threadPool.executor(ThreadPool.Names.SEARCH));
        this.transportService = transportService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, PrometheusQueryRequest request, ActionListener<PrometheusQueryResponse> listener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
        // the request may have waited for a thread long enough for its client to have gone away
        if (task instanceof CancellableTask cancellableTask && cancellableTask.notifyIfCancelled(listener)) {
            return;
        }
        PromqlStatementResult result = buildStatement(request.query());
        var esqlRequest = new PromqlQueryRequest(
            request.query().index(),
            result.esqlStatement(),
            request.description(),
            request.loggedParams()
        );
        new ParentTaskAssigningClient(client, transportService.getLocalNode(), task).execute(
            EsqlQueryAction.INSTANCE,
            esqlRequest,
            listener.map(esqlResponse -> new PrometheusQueryResponse(esqlResponse, result.resultType(), result.stringValue()))
        );
    }

    /**
     * Builds the ES|QL statement of a query, which is where its PromQL query or selectors are parsed.
     */
    static PromqlStatementResult buildStatement(PrometheusQuery query) {
        return switch (query) {
            case InstantQuery q -> PromqlQueryPlanBuilder.buildStatement(q.query(), q.index(), q.time(), QueryMode.INSTANT);
            case RangeQuery q -> PromqlQueryPlanBuilder.buildStatement(
                q.query(),
                q.index(),
                q.start(),
                q.end(),
                q.step(),
                q.limit(),
                QueryMode.RANGE
            );
            case ExemplarsQuery q -> PrometheusQueryExemplarsPlanBuilder.buildStatement(q.query(), q.index(), q.start(), q.end());
            case SeriesQuery q -> statement(
                PrometheusSeriesPlanBuilder.buildPlan(q.index(), q.matchSelectors(), q.start(), q.end(), q.limit())
            );
            case LabelsQuery q -> statement(
                PrometheusLabelsPlanBuilder.buildPlan(q.index(), q.matchSelectors(), q.start(), q.end(), q.limit())
            );
            case LabelValuesQuery q -> statement(
                PrometheusLabelValuesPlanBuilder.buildPlan(q.labelName(), q.index(), q.matchSelectors(), q.start(), q.end(), q.limit())
            );
            case MetadataQuery q -> statement(
                PrometheusMetadataPlanBuilder.buildPlan(q.index(), q.metric(), q.limit(), q.limitPerMetric())
            );
        };
    }

    private static PromqlStatementResult statement(LogicalPlan plan) {
        return new PromqlStatementResult(new EsqlStatement(plan, PrometheusPlanBuilderUtils.QUERY_SETTINGS));
    }
}
