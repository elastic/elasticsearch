/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecatedLogHandler;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.common.Strings.isNullOrEmpty;

/**
 * This service is responsible for writing deprecation messages to a data stream.
 * The writing of messages can be toggled using the
 * {@link #WRITE_DEPRECATION_LOGS_TO_INDEX} setting.
 */
public class DeprecationIndexingService extends AbstractLifecycleComponent implements ClusterStateListener, DeprecatedLogHandler {
    private static final String DATA_STREAM_NAME = "logs-deprecation-elasticsearch";

    public static final Setting<Boolean> WRITE_DEPRECATION_LOGS_TO_INDEX = Setting.boolSetting(
        "cluster.deprecation_indexing.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private final Consumer<IndexRequest> requestConsumer;

    private volatile boolean isEnabled = false;

    public DeprecationIndexingService(ClusterService clusterService, Consumer<IndexRequest> requestConsumer) {
        this.requestConsumer = requestConsumer;

        clusterService.addListener(this);
    }

    /**
     * Indexes a deprecation message.
     * @param key          the key that was used to determine if this deprecation should have been be logged.
     *                     Useful when aggregating the recorded messages.
     * @param esLogMessage the message to log
     */
    public void log(String key, String xOpaqueId, ESLogMessage esLogMessage) {
        if (this.lifecycle.started() == false) {
            return;
        }

        if (this.isEnabled == false) {
            return;
        }

        String messagePattern = esLogMessage.getMessagePattern();
        Object[] arguments = esLogMessage.getArguments();
        String message = LoggerMessageFormat.format(messagePattern, arguments);

        Map<String, Object> payload = new HashMap<>();
        payload.put("@timestamp", Instant.now().toString());
        payload.put("key", key);
        payload.put("message", message);

        if (isNullOrEmpty(xOpaqueId) == false) {
            payload.put("x-opaque-id", xOpaqueId);
        }

        final IndexRequest request = new IndexRequest(DATA_STREAM_NAME).source(payload).opType(DocWriteRequest.OpType.CREATE);

        this.requestConsumer.accept(request);
    }

    /**
     * Listens for changes to the cluster state, in order to know whether to toggle indexing.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        this.isEnabled = WRITE_DEPRECATION_LOGS_TO_INDEX.get(event.state().getMetadata().settings());
    }

    @Override
    protected void doStart() {
        DeprecationLogger.addHandler(this);
    }

    @Override
    protected void doStop() {
        DeprecationLogger.removeHandler(this);
    }

    @Override
    protected void doClose() {
        DeprecationLogger.removeHandler(this);
    }
}
