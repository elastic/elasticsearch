/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.LoggerMessageFormat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.common.Strings.isNullOrEmpty;
import static org.elasticsearch.common.logging.DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME;

/**
 * This log4j appender writes deprecation log messages to an index. It does not perform the actual
 * writes, but instead constructs an {@link IndexRequest} for the log message and passes that
 * to a callback.
 */
@Plugin(name = "DeprecationIndexingAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class DeprecationIndexingAppender extends AbstractAppender {
    private static final String DATA_STREAM_NAME = "logs-deprecation-elasticsearch";

    private final Consumer<IndexRequest> requestConsumer;
    private String clusterUUID;
    private String nodeId;

    /**
     * You can't start and stop an appender to toggle it, so this flag reflects whether
     * writes should in fact be carried out.
     */
    private volatile boolean isEnabled = false;

    /**
     * Creates a new appender.
     * @param name the appender's name
     * @param filter any filter to apply directly on the appender
     * @param requestConsumer a callback to handle the actual indexing of the log message.
     */
    public DeprecationIndexingAppender(String name, Filter filter, Consumer<IndexRequest> requestConsumer) {
        super(name, filter, null);
        this.requestConsumer = Objects.requireNonNull(requestConsumer, "requestConsumer cannot be null");
    }

    /**
     * Constructs an index request for a deprecation message, and supplies it to the callback that was
     * supplied to {@link #DeprecationIndexingAppender(String, Filter, Consumer)}.
     */
    @Override
    public void append(LogEvent event) {
        if (this.isEnabled == false) {
            return;
        }

        final Message message = event.getMessage();

        final Map<String, Object> payload = new HashMap<>();
        payload.put("@timestamp", Instant.now().toString());
        payload.put("cluster.uuid", clusterUUID);
        payload.put("node.id", nodeId);

        if (message instanceof ESLogMessage) {
            final ESLogMessage esLogMessage = (ESLogMessage) message;

            String xOpaqueId = esLogMessage.get(X_OPAQUE_ID_FIELD_NAME);

            payload.put("key", esLogMessage.get("key"));
            payload.put("message", esLogMessage.get("message"));

            if (isNullOrEmpty(xOpaqueId) == false) {
                payload.put("x-opaque-id", xOpaqueId);
            }
        } else {
            payload.put("message", message.getFormattedMessage());
        }

        final IndexRequest request = new IndexRequest(DATA_STREAM_NAME).source(payload).opType(DocWriteRequest.OpType.CREATE);

        this.requestConsumer.accept(request);
    }

    /**
     * Sets whether this appender is enabled or disabled.
     * @param isEnabled the enabled status of the appender.
     */
    public void setEnabled(boolean isEnabled) {
        this.isEnabled = isEnabled;
    }

    /**
     * Sets the current cluster UUID. This is included in the indexed document.
     * @param clusterUUID the cluster UUID to set.
     */
    public void setClusterUUID(String clusterUUID) {
        this.clusterUUID = clusterUUID;
    }

    /**
     * Sets the current node ID. This is included in the indexed document.
     * @param nodeId the node ID to set.
     */
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
