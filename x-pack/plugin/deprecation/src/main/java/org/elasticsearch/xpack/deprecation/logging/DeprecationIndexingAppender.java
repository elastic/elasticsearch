/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.RateLimiter;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.common.Strings.isNullOrEmpty;
import static org.elasticsearch.common.logging.DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME;

public class DeprecationIndexingAppender extends AbstractAppender {
    private static final String DATA_STREAM_NAME = "logs-deprecation-elasticsearch";

    private final Consumer<IndexRequest> requestConsumer;
    private final RateLimiter rateLimiter;
    private String clusterUUID;
    private String nodeId;

    private volatile boolean isEnabled = false;

    public DeprecationIndexingAppender(
        Consumer<IndexRequest> requestConsumer,
        String name,
        Filter filter
    ) {
        super(name, filter, null);
        this.requestConsumer = requestConsumer;
        this.rateLimiter = new RateLimiter();
    }

    /**
     * Indexes a deprecation message.
     */
    @Override
    public void append(LogEvent event) {
        if (this.isEnabled == false) {
            return;
        }

        final Message message = event.getMessage();
        if ((message instanceof ESLogMessage) == false) {
            return;
        }

        final ESLogMessage esLogMessage = (ESLogMessage) message;

        String xOpaqueId = esLogMessage.get(X_OPAQUE_ID_FIELD_NAME);
        final String key = esLogMessage.get("x-key");

        this.rateLimiter.limit(xOpaqueId + key, () -> {
            String messagePattern = esLogMessage.getMessagePattern();
            Object[] arguments = esLogMessage.getArguments();
            String formattedMessage = LoggerMessageFormat.format(messagePattern, arguments);

            Map<String, Object> payload = new HashMap<>();
            payload.put("@timestamp", Instant.now().toString());
            payload.put("key", key);
            payload.put("message", formattedMessage);
            payload.put("cluster.uuid", clusterUUID);
            payload.put("node.id", nodeId);

            if (isNullOrEmpty(xOpaqueId) == false) {
                payload.put("x-opaque-id", xOpaqueId);
            }

            final IndexRequest request = new IndexRequest(DATA_STREAM_NAME).source(payload).opType(DocWriteRequest.OpType.CREATE);

            this.requestConsumer.accept(request);
        });
    }

    public void setEnabled(boolean isEnabled) {
        this.isEnabled = isEnabled;
    }

    public void setClusterUUID(String clusterUUID) {
        this.clusterUUID = clusterUUID;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
