/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.execute;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.ManualExecutionContext;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
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
    private final Client client;

    @Inject
    public TransportExecuteWatchAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       ExecutionService executionService, Clock clock, XPackLicenseState licenseState,
                                       Watch.Parser watchParser, InternalClient client, TriggerService triggerService,
                                       ClusterService clusterService) {
        super(settings, ExecuteWatchAction.NAME, transportService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, clusterService, ExecuteWatchRequest::new, ExecuteWatchResponse::new);
        this.executionService = executionService;
        this.clock = clock;
        this.triggerService = triggerService;
        this.watchParser = watchParser;
        this.client = client;
    }

    @Override
    protected void masterOperation(ExecuteWatchRequest request, ClusterState state,
                                   ActionListener<ExecuteWatchResponse> listener) throws Exception {
        if (request.getId() != null) {
            GetRequest getRequest = new GetRequest(Watch.INDEX, Watch.DOC_TYPE, request.getId())
                    .preference(Preference.LOCAL.type()).realtime(true);

            client.get(getRequest, ActionListener.wrap(response -> {
                if (response.isExists()) {
                    Watch watch = watchParser.parse(request.getId(), true, response.getSourceAsBytesRef(), request.getXContentType());
                    watch.version(response.getVersion());
                    watch.status().version(response.getVersion());
                    executeWatch(request, listener, watch, true);
                } else {
                    listener.onFailure(new ResourceNotFoundException("Watch with id [{}] does not exist", request.getId()));
                }
            }, listener::onFailure));
        } else if (request.getWatchSource() != null) {
            try {
                assert !request.isRecordExecution();
                Watch watch = watchParser.parse(ExecuteWatchRequest.INLINE_WATCH_ID, true, request.getWatchSource(),
                request.getXContentType());
                executeWatch(request, listener, watch, false);
            } catch (IOException e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to parse [{}]", request.getId()), e);
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new IllegalArgumentException("no watch provided"));
        }
    }

    private void executeWatch(ExecuteWatchRequest request, ActionListener<ExecuteWatchResponse> listener,
                              Watch watch, boolean knownWatch) {

        threadPool.executor(XPackPlugin.WATCHER).submit(() -> {
            try {
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
                listener.onResponse(new ExecuteWatchResponse(record.id().value(), builder.bytes(), XContentType.JSON));
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

}
