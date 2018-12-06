/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.notifications.AuditMessage;
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class Auditor {

    private static final Logger LOGGER = LogManager.getLogger(Auditor.class);

    private final Client client;
    private final String nodeName;

    public Auditor(Client client, String nodeName) {
        this.client = Objects.requireNonNull(client);
        this.nodeName = Objects.requireNonNull(nodeName);
    }

    public void info(String jobId, String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newInfo(jobId, message, nodeName));
    }

    public void warning(String jobId, String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newWarning(jobId, message, nodeName));
    }

    public void error(String jobId, String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newError(jobId, message, nodeName));
    }

    private void indexDoc(String type, ToXContent toXContent) {
        IndexRequest indexRequest = new IndexRequest(AuditorField.NOTIFICATIONS_INDEX, type);
        indexRequest.source(toXContentBuilder(toXContent));
        indexRequest.timeout(TimeValue.timeValueSeconds(5));
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOGGER.trace("Successfully persisted {}", type);
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.debug(new ParameterizedMessage("Error writing {}", new Object[]{type}, e));
            }
        }, client::index);
    }

    private XContentBuilder toXContentBuilder(ToXContent toXContent) {
        try (XContentBuilder jsonBuilder = jsonBuilder()) {
            return toXContent.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
