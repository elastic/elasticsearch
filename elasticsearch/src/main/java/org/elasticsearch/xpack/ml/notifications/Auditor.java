/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Auditor {

    public static final String NOTIFICATIONS_INDEX = ".ml-notifications";
    private static final Logger LOGGER = Loggers.getLogger(Auditor.class);

    private final Client client;
    private final String jobId;

    public Auditor(Client client, String jobId) {
        this.client = Objects.requireNonNull(client);
        this.jobId = jobId;
    }

    public void info(String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newInfo(jobId, message));
    }

    public void warning(String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newWarning(jobId, message));
    }

    public void error(String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newError(jobId, message));
    }

    public void activity(int totalJobs, int totalDetectors, int runningJobs, int runningDetectors) {
        String type = AuditActivity.TYPE.getPreferredName();
        indexDoc(type, AuditActivity.newActivity(totalJobs, totalDetectors, runningJobs, runningDetectors));
    }

    private void indexDoc(String type, ToXContent toXContent) {
        client.prepareIndex(NOTIFICATIONS_INDEX, type)
                .setSource(toXContentBuilder(toXContent))
                .execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        LOGGER.trace("Successfully persisted {}", type);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.error(new ParameterizedMessage("Error writing {}", new Object[]{true}, e));
                    }
                });
    }

    private XContentBuilder toXContentBuilder(ToXContent toXContent) {
        try {
            return toXContent.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
