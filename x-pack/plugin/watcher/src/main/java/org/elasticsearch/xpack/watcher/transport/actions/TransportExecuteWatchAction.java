/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.ClockHolder;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.ManualExecutionContext;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.WatchParser;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Performs the watch execution operation.
 */
public class TransportExecuteWatchAction extends WatcherTransportAction<ExecuteWatchRequest, ExecuteWatchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportExecuteWatchAction.class);

    private final ThreadPool threadPool;
    private final ExecutionService executionService;
    private final Clock clock;
    private final TriggerService triggerService;
    private final WatchParser watchParser;
    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportExecuteWatchAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ExecutionService executionService,
        ClockHolder clockHolder,
        XPackLicenseState licenseState,
        WatchParser watchParser,
        Client client,
        TriggerService triggerService,
        ClusterService clusterService
    ) {
        super(ExecuteWatchAction.NAME, transportService, actionFilters, licenseState, ExecuteWatchRequest::new);
        this.threadPool = threadPool;
        this.executionService = executionService;
        this.clock = clockHolder.clock;
        this.triggerService = triggerService;
        this.watchParser = watchParser;
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(ExecuteWatchRequest request, ActionListener<ExecuteWatchResponse> listener) {
        if (request.getId() != null) {
            GetRequest getRequest = new GetRequest(Watch.INDEX, request.getId()).preference(Preference.LOCAL.type()).realtime(true);

            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                WATCHER_ORIGIN,
                getRequest,
                ActionListener.<GetResponse>wrap(response -> {
                    if (response.isExists()) {
                        Watch watch = watchParser.parse(
                            request.getId(),
                            true,
                            response.getSourceAsBytesRef(),
                            request.getXContentType(),
                            response.getSeqNo(),
                            response.getPrimaryTerm()
                        );
                        watch.status().version(response.getVersion());
                        executeWatch(request, listener, watch, true);
                    } else {
                        listener.onFailure(new ResourceNotFoundException("Watch with id [{}] does not exist", request.getId()));
                    }
                }, listener::onFailure),
                client::get
            );
        } else if (request.getWatchSource() != null) {
            try {
                assert request.isRecordExecution() == false;
                Watch watch = watchParser.parse(
                    ExecuteWatchRequest.INLINE_WATCH_ID,
                    true,
                    request.getWatchSource(),
                    request.getXContentType(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                );
                executeWatch(request, listener, watch, false);
            } catch (IOException e) {
                logger.error(() -> "failed to parse [" + request.getId() + "]", e);
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new IllegalArgumentException("no watch provided"));
        }
    }

    private void executeWatch(
        final ExecuteWatchRequest request,
        final ActionListener<ExecuteWatchResponse> listener,
        final Watch watch,
        final boolean knownWatch
    ) {
        try {
            /*
             * Ensure that the headers from the incoming request are used instead those of the stored watch otherwise the watch would run
             * as the user who stored the watch, but it needs to run as the user who executes this request.
             */
            watch.status()
                .setHeaders(ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), clusterService.state()));

            final String triggerType = watch.trigger().type();
            final TriggerEvent triggerEvent = triggerService.simulateEvent(triggerType, watch.id(), request.getTriggerData());

            final ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(
                watch,
                knownWatch,
                new ManualTriggerEvent(triggerEvent.jobName(), triggerEvent),
                executionService.defaultThrottlePeriod()
            );

            final ZonedDateTime executionTime = clock.instant().atZone(ZoneOffset.UTC);
            ctxBuilder.executionTime(executionTime);
            for (final Map.Entry<String, ActionExecutionMode> entry : request.getActionModes().entrySet()) {
                ctxBuilder.actionMode(entry.getKey(), entry.getValue());
            }
            if (request.getAlternativeInput() != null) {
                ctxBuilder.withInput(new SimpleInput.Result(new Payload.Simple(request.getAlternativeInput())));
            }
            if (request.isIgnoreCondition()) {
                ctxBuilder.withCondition(InternalAlwaysCondition.RESULT_INSTANCE);
            }
            ctxBuilder.recordExecution(request.isRecordExecution());
            final WatchExecutionContext ctx = ctxBuilder.build();

            // use execute so that the runnable is not wrapped in a RunnableFuture<?>
            threadPool.executor(XPackField.WATCHER).execute(new ExecutionService.WatchExecutionTask(ctx, new AbstractRunnable() {

                @Override
                public void onFailure(final Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() throws Exception {
                    final WatchRecord record = executionService.execute(ctx);
                    final XContentBuilder builder = XContentFactory.jsonBuilder();

                    record.toXContent(builder, WatcherParams.builder().hideSecrets(true).debug(request.isDebug()).build());
                    listener.onResponse(new ExecuteWatchResponse(record.id().value(), BytesReference.bytes(builder), XContentType.JSON));
                }

            }));
        } catch (final Exception e) {
            listener.onFailure(e);
        }

    }
}
