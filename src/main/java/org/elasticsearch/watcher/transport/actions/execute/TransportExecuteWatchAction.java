/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.execute;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.execution.ManualExecutionContext;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStore;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
/**
 * Performs the watch execution operation.
 */
public class TransportExecuteWatchAction extends WatcherTransportAction<ExecuteWatchRequest, ExecuteWatchResponse> {

    private final ExecutionService executionService;
    private final WatchStore watchStore;
    private final Clock clock;

    @Inject
    public TransportExecuteWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters, ExecutionService executionService,
                                       Clock clock, LicenseService licenseService, WatchStore watchStore) {
        super(settings, ExecuteWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, licenseService);
        this.executionService = executionService;
        this.watchStore = watchStore;
        this.clock = clock;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ExecuteWatchRequest newRequest() {
        return new ExecuteWatchRequest();
    }

    @Override
    protected ExecuteWatchResponse newResponse() {
        return new ExecuteWatchResponse();
    }

    @Override
    protected void masterOperation(ExecuteWatchRequest request, ClusterState state, ActionListener<ExecuteWatchResponse> listener) throws ElasticsearchException {
        try {
            Watch watch = watchStore.get(request.getId());
            if (watch == null) {
                throw new WatcherException("watch [" + request.getId() + "] does not exist");
            }

            ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(watch);
            DateTime executionTime = clock.now(UTC);
            ctxBuilder.executionTime(executionTime);
            if (request.isSimulateAllActions()) {
                ctxBuilder.simulateAllActions();
            } else {
                ctxBuilder.simulateActions(request.getSimulatedActionIds().toArray(new String[request.getSimulatedActionIds().size()]));
            }
            if (request.getTriggerData() != null) {
                ctxBuilder.triggerEvent(new ManualTriggerEvent(watch.id(), executionTime, request.getTriggerData()));
            }
            if (request.getAlternativeInput() != null) {
                ctxBuilder.withInput(new SimpleInput.Result(new Payload.Simple(request.getAlternativeInput())));
            }
            if (request.isIgnoreCondition()) {
                ctxBuilder.withCondition(AlwaysCondition.Result.INSTANCE);
            }
            if (request.isIgnoreThrottle()) {
                ctxBuilder.withThrottle(Throttler.Result.NO);
            }
            ctxBuilder.recordExecution(request.isRecordExecution());

            WatchRecord record = executionService.execute(ctxBuilder.build());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            record.toXContent(builder, WatcherParams.builder().hideSecrets(true).build());
            ExecuteWatchResponse response = new ExecuteWatchResponse(builder.bytes());
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("failed to execute [{}]", e, request.getId());
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ExecuteWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, WatchStore.INDEX);
    }


}
