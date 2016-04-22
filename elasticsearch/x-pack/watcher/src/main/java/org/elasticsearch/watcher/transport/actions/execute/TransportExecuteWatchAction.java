/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.execute;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.execution.ActionExecutionMode;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.execution.ManualExecutionContext;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.WatcherLicensee;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.TriggerService;
import org.elasticsearch.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Map;

import static org.elasticsearch.watcher.support.Exceptions.illegalArgument;

/**
 * Performs the watch execution operation.
 */
public class TransportExecuteWatchAction extends WatcherTransportAction<ExecuteWatchRequest, ExecuteWatchResponse> {

    private final ExecutionService executionService;
    private final WatchStore watchStore;
    private final Clock clock;
    private final TriggerService triggerService;
    private final Watch.Parser watchParser;

    @Inject
    public TransportExecuteWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver, ExecutionService executionService,
                                       Clock clock, WatcherLicensee watcherLicensee, WatchStore watchStore, TriggerService triggerService,
                                       Watch.Parser watchParser) {
        super(settings, ExecuteWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                watcherLicensee, ExecuteWatchRequest::new);
        this.executionService = executionService;
        this.watchStore = watchStore;
        this.clock = clock;
        this.triggerService = triggerService;
        this.watchParser = watchParser;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ExecuteWatchResponse newResponse() {
        return new ExecuteWatchResponse();
    }

    @Override
    protected void masterOperation(ExecuteWatchRequest request, ClusterState state, ActionListener<ExecuteWatchResponse> listener)
            throws ElasticsearchException {
        try {
            Watch watch;
            boolean knownWatch;
            if (request.getId() != null) {
                watch = watchStore.get(request.getId());
                if (watch == null) {
                    //todo we need to find a better std exception for this one
                    throw new ElasticsearchException("watch [{}] does not exist", request.getId());
                }
                knownWatch = true;
            } else if (request.getWatchSource() != null) {
                assert !request.isRecordExecution();
                watch = watchParser.parse(ExecuteWatchRequest.INLINE_WATCH_ID, false, request.getWatchSource());
                knownWatch = false;
            } else {
                throw illegalArgument("no watch provided");
            }

            String triggerType = watch.trigger().type();
            TriggerEvent triggerEvent = triggerService.simulateEvent(triggerType, watch.id(), request.getTriggerData());

            ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(watch, knownWatch,
                    new ManualTriggerEvent(triggerEvent.jobName(), triggerEvent), executionService.defaultThrottlePeriod());

            DateTime executionTime = clock.now(DateTimeZone.UTC);
            ctxBuilder.executionTime(executionTime);
            for (Map.Entry<String, ActionExecutionMode> entry : request.getActionModes().entrySet()) {
                ctxBuilder.actionMode(entry.getKey(), entry.getValue());
            }
            if (request.getAlternativeInput() != null) {
                ctxBuilder.withInput(new SimpleInput.Result(new Payload.Simple(request.getAlternativeInput())));
            }
            if (request.isIgnoreCondition()) {
                ctxBuilder.withCondition(AlwaysCondition.Result.INSTANCE);
            }
            ctxBuilder.recordExecution(request.isRecordExecution());

            WatchRecord record = executionService.execute(ctxBuilder.build());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            record.toXContent(builder, WatcherParams.builder().hideSecrets(true).debug(request.isDebug()).build());
            ExecuteWatchResponse response = new ExecuteWatchResponse(record.id().value(), builder.bytes(), XContentType.JSON);
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
