/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecatedLogHandler;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Setting;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.Strings.isNullOrEmpty;

/**
 * This service is responsible for writing deprecation messages to a data stream.
 * The writing of messages can be toggled using the
 * {@link #WRITE_DEPRECATION_LOGS_TO_INDEX} setting.
 */
public class DeprecationIndexingService extends AbstractLifecycleComponent implements ClusterStateListener, DeprecatedLogHandler {
    private static final Logger LOGGER = LogManager.getLogger(DeprecationIndexingService.class);

    private static final String DATA_STREAM_NAME = "logs-deprecation-elasticsearch";

    private static final String DEPRECATION_ORIGIN = "deprecation";

    public static final Setting<Boolean> WRITE_DEPRECATION_LOGS_TO_INDEX = Setting.boolSetting(
        "cluster.deprecation_indexing.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Client client;
    private volatile boolean isEnabled = false;

    public DeprecationIndexingService(ClusterService clusterService, Client client) {
        this.client = new OriginSettingClient(client, DEPRECATION_ORIGIN);

        clusterService.addListener(this);
    }

    /**
     * Indexes a deprecation message.
     * @param key          the key that was used to determine if this deprecation should have been be logged.
     *                     Useful when aggregating the recorded messages.
     * @param esLogMessage the message to log
     */
    public void log(String key, String xOpaqueId, ESLogMessage esLogMessage) {
        if (this.lifecycle.started() == false)  {
            return;
        }

        if (this.isEnabled == false) {
            return;
        }

        String message = esLogMessage.getMessagePattern();
        Object[] params = esLogMessage.getArguments();

        Map<String, Object> payload = new HashMap<>();
        payload.put("@timestamp", Instant.now().toString());
        payload.put("key", key);
        payload.put("message", message);

        if (isNullOrEmpty(xOpaqueId) == false) {
            payload.put("x-opaque-id", xOpaqueId);
        }

        if (params != null && params.length > 0) {
            payload.put("params", params);
        }

        new IndexRequestBuilder(client, IndexAction.INSTANCE).setIndex(DATA_STREAM_NAME)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(payload)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    // Nothing to do
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("Failed to index deprecation message", e);
                }
            });
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
