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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
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
    private final ClusterService clusterService;

    public Auditor(Client client, ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.clusterService = clusterService;
    }

    public void info(String jobId, String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newInfo(jobId, message, clusterService.localNode().getName()));
    }

    public void warning(String jobId, String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newWarning(jobId, message, clusterService.localNode().getName()));
    }

    public void error(String jobId, String message) {
        indexDoc(AuditMessage.TYPE.getPreferredName(), AuditMessage.newError(jobId, message, clusterService.localNode().getName()));
    }

    private void indexDoc(String type, ToXContent toXContent) {
        // TODO: (norelease): Fix the assertion tripping in internal engine for index requests without an id being retried:
        client.prepareIndex(NOTIFICATIONS_INDEX, type, UUIDs.base64UUID())
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
