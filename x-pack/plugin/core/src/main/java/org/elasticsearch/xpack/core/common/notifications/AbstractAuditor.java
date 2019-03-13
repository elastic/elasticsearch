/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public abstract class AbstractAuditor<T extends AbstractAuditMessage> {

    private final Client client;
    private final String nodeName;
    private final AbstractAuditMessage.AuditMessageBuilder<T> messageBuilder;

    public AbstractAuditor(Client client, String nodeName, AbstractAuditMessage.AuditMessageBuilder<T> messageBuilder) {
        this.client = Objects.requireNonNull(client);
        this.nodeName = Objects.requireNonNull(nodeName);
        this.messageBuilder = Objects.requireNonNull(messageBuilder);
    }

    public final void info(String resourceId, String message) {
        indexDoc(messageBuilder.info(resourceId, message, nodeName));
    }

    public final void warning(String resourceId, String message) {
        indexDoc(messageBuilder.warning(resourceId, message, nodeName));
    }

    public final void error(String resourceId, String message) {
        indexDoc(messageBuilder.error(resourceId, message, nodeName));
    }

    protected abstract String getExecutionOrigin();

    protected abstract String getAuditIndex();

    protected abstract void onIndexResponse(IndexResponse response);

    protected abstract void onIndexFailure(Exception exception);

    private void indexDoc(ToXContent toXContent) {
        IndexRequest indexRequest = new IndexRequest(getAuditIndex());
        indexRequest.source(toXContentBuilder(toXContent));
        indexRequest.timeout(TimeValue.timeValueSeconds(5));
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            getExecutionOrigin(),
            indexRequest,
            ActionListener.wrap(
                this::onIndexResponse,
                this::onIndexFailure
            ), client::index);
    }

    private XContentBuilder toXContentBuilder(ToXContent toXContent) {
        try (XContentBuilder jsonBuilder = jsonBuilder()) {
            return toXContent.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
