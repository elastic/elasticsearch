/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractAuditor<T extends AbstractAuditMessage> {

    private static final Logger logger = LogManager.getLogger(AbstractAuditor.class);
    private final OriginSettingClient client;
    private final String nodeName;
    private final String auditIndex;
    private final String templateName;
    private final AbstractAuditMessageFactory<T> messageFactory;
    private final AtomicBoolean hasLatestTemplate;

    private Queue<ToXContent> backlog;
    private ClusterService clusterService;


    protected AbstractAuditor(OriginSettingClient client,
                              String nodeName,
                              String auditIndex,
                              String templateName,
                              AbstractAuditMessageFactory<T> messageFactory,
                              ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.nodeName = Objects.requireNonNull(nodeName);
        this.auditIndex = auditIndex;
        this.templateName = Objects.requireNonNull(templateName);
        this.messageFactory = Objects.requireNonNull(messageFactory);
        this.clusterService = clusterService;
        this.hasLatestTemplate = new AtomicBoolean();
        this.backlog = new ConcurrentLinkedQueue<>();

    }

    public void info(String resourceId, String message) {
        indexDoc(messageFactory.newMessage(resourceId, message, Level.INFO, new Date(), nodeName));
    }

    public void warning(String resourceId, String message) {
        indexDoc(messageFactory.newMessage(resourceId, message, Level.WARNING, new Date(), nodeName));
    }

    public void error(String resourceId, String message) {
        indexDoc(messageFactory.newMessage(resourceId, message, Level.ERROR, new Date(), nodeName));
    }

    private void onIndexResponse(IndexResponse response) {
        logger.trace("Successfully wrote audit message");
    }

    private void onIndexFailure(Exception exception) {
        logger.debug("Failed to write audit message", exception);
    }

    private void writeDoc(ToXContent toXContent) {
        if (hasLatestTemplate.get()) {
            indexDoc(toXContent);
            return;
        }

        ActionListener<Boolean> putTemplateListener = ActionListener.wrap(
            r -> {
                synchronized (this) {
                    this.hasLatestTemplate.set(true);
                }
                logger.info("Auditor template [{}] successfully installed", templateName);
                writeBacklog();
            },
            e -> {
                logger.warn("Error putting latest template [{}]", templateName);
            }
        );

        synchronized (this) {
            if (hasLatestTemplate.get() == false) {
                // synchronized so that hasLatestTemplate does not change value
                // between the read and adding to the backlog
                backlog.add(toXContent);
                MlIndexAndAlias.installIndexTemplateIfRequired(clusterService.state(), client, templateName,
                    putTemplateListener);
                return;
            }
        }

        indexDoc(toXContent);
     }

    private void indexDoc(ToXContent toXContent) {
        client.index(indexRequest(toXContent), ActionListener.wrap(
            this::onIndexResponse,
            this::onIndexFailure
        ));
    }

    private IndexRequest indexRequest(ToXContent toXContent) {
        IndexRequest indexRequest = new IndexRequest(auditIndex);
        indexRequest.source(toXContentBuilder(toXContent));
        indexRequest.timeout(TimeValue.timeValueSeconds(5));
        return indexRequest;
    }

    private XContentBuilder toXContentBuilder(ToXContent toXContent) {
        try (XContentBuilder jsonBuilder = jsonBuilder()) {
            return toXContent.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeBacklog() {
        BulkRequest bulkRequest = new BulkRequest();
        for (ToXContent toXContent : backlog) {
            bulkRequest.add(indexRequest(toXContent));
        }

        client.bulk(bulkRequest, ActionListener.wrap(
            bulkItemResponses -> {
                backlog = null;
                logger.trace("Successfully wrote audit message backlog after upgrading template");
            },
            this::onIndexFailure
        ));
    }
}
