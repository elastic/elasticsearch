/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentType;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * This log4j appender writes deprecation log messages to an index. It does not perform the actual
 * writes, but instead constructs an {@link IndexRequest} for the log message and passes that
 * to a callback.
 */
@Plugin(name = "DeprecationIndexingAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class DeprecationIndexingAppender extends AbstractAppender {
    private static final Logger logger = LogManager.getLogger(DeprecationIndexingAppender.class);
    public static final String DEPRECATION_MESSAGES_DATA_STREAM = ".logs-elasticsearch.deprecation-default";

    private final Consumer<IndexRequest> requestConsumer;

    /**
     * You can't start and stop an appender to toggle it, so this flag reflects whether
     * writes should in fact be carried out.
     */
    private volatile boolean isEnabled = false;

    /**
     * Creates a new appender.
     *
     * @param name            the appender's name
     * @param filter          a filter to apply directly on the appender
     * @param layout          the layout to use for formatting message. It must return a JSON string.
     * @param requestConsumer a callback to handle the actual indexing of the log message.
     */
    public DeprecationIndexingAppender(String name, Filter filter, Layout<String> layout, Consumer<IndexRequest> requestConsumer) {
        super(name, filter, layout);
        this.requestConsumer = Objects.requireNonNull(requestConsumer, "requestConsumer cannot be null");
    }

    /**
     * Constructs an index request for a deprecation message, and passes it to the callback that was
     * supplied to {@link #DeprecationIndexingAppender(String, Filter, Layout, Consumer)}.
     */
    @Override
    public void append(LogEvent event) {
        logger.trace(
            () -> Strings.format(
                "Received deprecation log event. Appender is %s. message = %s",
                isEnabled ? "enabled" : "disabled",
                event.getMessage().getFormattedMessage()
            )
        );
        if (this.isEnabled == false) {
            return;
        }

        final byte[] payload = this.getLayout().toByteArray(event);

        final IndexRequest request = new IndexRequest(DEPRECATION_MESSAGES_DATA_STREAM).source(payload, XContentType.JSON)
            .opType(DocWriteRequest.OpType.CREATE);

        this.requestConsumer.accept(request);
    }

    /**
     * Sets whether this appender is enabled or disabled. When disabled, the appender will
     * not perform indexing operations.
     *
     * @param enabled the enabled status of the appender.
     */
    public void setEnabled(boolean enabled) {
        this.isEnabled = enabled;
    }

    /**
     * Returns whether the appender is enabled i.e. performing indexing operations.
     */
    public boolean isEnabled() {
        return isEnabled;
    }
}
