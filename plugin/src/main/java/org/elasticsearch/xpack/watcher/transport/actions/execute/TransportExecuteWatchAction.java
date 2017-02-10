/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.execute;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.ManualExecutionContext;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;

import static org.joda.time.DateTimeZone.UTC;

/**
 * Performs the watch execution operation.
 */
public class TransportExecuteWatchAction extends WatcherTransportAction<ExecuteWatchRequest, ExecuteWatchResponse> {

    private final ExecutionService executionService;
    private final Clock clock;
    private final TriggerService triggerService;
    private final Watch.Parser watchParser;
    private final WatcherClientProxy client;

    @Inject
    public TransportExecuteWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver, ExecutionService executionService,
                                       Clock clock, XPackLicenseState licenseState, TriggerService triggerService,
                                       Watch.Parser watchParser, WatcherClientProxy client) {
        super(settings, ExecuteWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, ExecuteWatchRequest::new);
        this.executionService = executionService;
        this.clock = clock;
        this.triggerService = triggerService;
        this.watchParser = watchParser;
        this.client = client;
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
        if (request.getId() != null) {
            try {
                // should be executed async in the future
                GetResponse getResponse = client.getWatch(request.getId());
                Watch watch = watchParser.parse(request.getId(), true, getResponse.getSourceAsBytesRef(), XContentType.JSON);
                watch.version(getResponse.getVersion());
                watch.status().version(getResponse.getVersion());
                ExecuteWatchResponse executeWatchResponse = executeWatch(request, watch, true);
                listener.onResponse(executeWatchResponse);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        } else if (request.getWatchSource() != null) {
            try {
                assert !request.isRecordExecution();
                Watch watch =
                        watchParser.parse(ExecuteWatchRequest.INLINE_WATCH_ID, true, request.getWatchSource(), request.getXContentType());
                ExecuteWatchResponse response = executeWatch(request, watch, false);
                listener.onResponse(response);
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to execute [{}]", request.getId()), e);
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new IllegalArgumentException("no watch provided"));
        }
    }

    private ExecuteWatchResponse executeWatch(ExecuteWatchRequest request, Watch watch, boolean knownWatch) throws IOException {
        String triggerType = watch.trigger().type();
        TriggerEvent triggerEvent = triggerService.simulateEvent(triggerType, watch.id(), request.getTriggerData());

        ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(watch, knownWatch,
                new ManualTriggerEvent(triggerEvent.jobName(), triggerEvent), executionService.defaultThrottlePeriod());

        DateTime executionTime = new DateTime(clock.millis(), UTC);
        ctxBuilder.executionTime(executionTime);
        for (Map.Entry<String, ActionExecutionMode> entry : request.getActionModes().entrySet()) {
            ctxBuilder.actionMode(entry.getKey(), entry.getValue());
        }
        if (request.getAlternativeInput() != null) {
            ctxBuilder.withInput(new SimpleInput.Result(new Payload.Simple(request.getAlternativeInput())));
        }
        if (request.isIgnoreCondition()) {
            ctxBuilder.withCondition(AlwaysCondition.RESULT_INSTANCE);
        }
        ctxBuilder.recordExecution(request.isRecordExecution());

        WatchRecord record = executionService.execute(ctxBuilder.build());
        XContentBuilder builder = XContentFactory.jsonBuilder();

        record.toXContent(builder, WatcherParams.builder().hideSecrets(true).debug(request.isDebug()).build());
        return new ExecuteWatchResponse(record.id().value(), builder.bytes(), XContentType.JSON);
    }

    @Override
    protected ClusterBlockException checkBlock(ExecuteWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, Watch.INDEX);
    }


}
