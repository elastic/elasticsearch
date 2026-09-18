/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.utils.StatelessPrimaryRelocationMetricsCollectorProvider;

import java.util.concurrent.Executor;

import static org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction.TYPE;

/// [TransportAction] for starting a stateless primary relocation.
///
/// Invoked by the target node via the [PeerRecoveryTargetService], and the request goes to the source node. The
/// source-side handler delegates to [StatelessPrimaryRelocationSourceService].
public class TransportStatelessPrimaryRelocationAction extends TransportAction<
    StatelessPrimaryRelocationAction.Request,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportStatelessPrimaryRelocationAction.class);

    public static final String START_RELOCATION_ACTION_NAME = TYPE.name() + "/start";

    public static final Setting<TimeValue> SLOW_RELOCATION_THRESHOLD_SETTING = Setting.timeSetting(
        "stateless.cluster.primary_relocation.slow_handoff_warning_threshold",
        TimeValue.timeValueSeconds(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> ID_LOOKUP_RECENCY_THRESHOLD_SETTING = Setting.timeSetting(
        "stateless.cluster.primary_relocation.id_lookup_recency_threshold",
        TimeValue.timeValueMinutes(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;
    private final Executor recoveryExecutor;
    private final StatelessPrimaryRelocationMetricsCollectorProvider relocationMetricsCollectorProvider;

    @Inject
    public TransportStatelessPrimaryRelocationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        CompositeRecoverySchedulingListener recoverySchedulingListeners,
        StatelessPrimaryRelocationSourceService primaryRelocationSourceService,
        PeerRecoveryTargetService peerRecoveryTargetService,
        StatelessPrimaryRelocationMetricsCollectorProvider relocationMetricsCollectorProvider
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.peerRecoveryTargetService = peerRecoveryTargetService;
        this.relocationMetricsCollectorProvider = relocationMetricsCollectorProvider;
        this.recoveryExecutor = transportService.getThreadPool().generic();

        primaryRelocationSourceService.registerRecoverySchedulingListeners(recoverySchedulingListeners);

        transportService.registerRequestHandler(
            START_RELOCATION_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            StatelessPrimaryRelocationAction.Request::new,
            (request, channel, task) -> primaryRelocationSourceService.startRelocation(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doExecute(Task task, StatelessPrimaryRelocationAction.Request request, ActionListener<ActionResponse.Empty> listener) {
        // executed locally by `PeerRecoveryTargetService` (i.e. we are on the target node here)
        logger.trace("{} preparing unsearchable shard for primary relocation", request.shardId());

        try (var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.recoveryId(), request.shardId())) {
            final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            final var indexShard = indexService.getShard(request.shardId().id());
            indexShard.ensureRecoveryNotCancelled();
            indexShard.prepareForIndexRecovery();

            transportService.sendChildRequest(
                recoveryRef.target().sourceNode(),
                START_RELOCATION_ACTION_NAME,
                request,
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener.map(response -> {
                    // We record the source metrics on the target node because once the source receives a SIGTERM
                    // the metrics agent stops emitting metrics and we lose all that information
                    RelocationSourceMetrics relocationSourceMetrics = response.getRelocationSourceMetrics();
                    if (relocationSourceMetrics != null) {
                        relocationMetricsCollectorProvider.get().recordRelocationSourceMetrics(relocationSourceMetrics);
                    }
                    return ActionResponse.Empty.INSTANCE;
                }), StartRelocationResponse::new, recoveryExecutor)
            );
        }
    }

}
