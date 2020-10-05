/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractAuditor<T extends AbstractAuditMessage> {

    private static final Logger logger = LogManager.getLogger(AbstractAuditor.class);
    static final int MAX_BUFFER_SIZE = 1000;

    private final OriginSettingClient client;
    private final String nodeName;
    private final String auditIndex;
    private final String templateName;
    private final Supplier<PutIndexTemplateRequest> templateSupplier;
    private final AbstractAuditMessageFactory<T> messageFactory;
    private final AtomicBoolean hasLatestTemplate;

    private Queue<ToXContent> backlog;
    private final ClusterService clusterService;
    private final AtomicBoolean putTemplateInProgress;


    protected AbstractAuditor(OriginSettingClient client,
                              String auditIndex,
                              IndexTemplateConfig templateConfig,
                              String nodeName,
                              AbstractAuditMessageFactory<T> messageFactory,
                              ClusterService clusterService) {

        this(client, auditIndex, templateConfig.getTemplateName(),
            () -> new PutIndexTemplateRequest(templateConfig.getTemplateName()).source(templateConfig.loadBytes(), XContentType.JSON),
            nodeName, messageFactory, clusterService);
    }


    protected AbstractAuditor(OriginSettingClient client,
                              String auditIndex,
                              String templateName,
                              Supplier<PutIndexTemplateRequest> templateSupplier,
                              String nodeName,
                              AbstractAuditMessageFactory<T> messageFactory,
                              ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.auditIndex = Objects.requireNonNull(auditIndex);
        this.templateName = Objects.requireNonNull(templateName);
        this.templateSupplier = Objects.requireNonNull(templateSupplier);
        this.messageFactory = Objects.requireNonNull(messageFactory);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.nodeName = Objects.requireNonNull(nodeName);
        this.backlog = new ConcurrentLinkedQueue<>();
        this.hasLatestTemplate = new AtomicBoolean();
        this.putTemplateInProgress = new AtomicBoolean();
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

    private void indexDoc(ToXContent toXContent) {
        if (hasLatestTemplate.get()) {
            writeDoc(toXContent);
            return;
        }

        if (MlIndexAndAlias.hasIndexTemplate(clusterService.state(), templateName)) {
            synchronized (this) {
                // synchronized so nothing can be added to backlog while this value changes
                hasLatestTemplate.set(true);
            }
            writeDoc(toXContent);
            return;
        }

        ActionListener<Boolean> putTemplateListener = ActionListener.wrap(
            r -> {
                synchronized (this) {
                    // synchronized so nothing can be added to backlog while this value changes
                    hasLatestTemplate.set(true);
                }
                logger.info("Auditor template [{}] successfully installed", templateName);
                writeBacklog();
                putTemplateInProgress.set(false);
            },
            e -> {
                logger.warn("Error putting latest template [{}]", templateName);
                putTemplateInProgress.set(false);
            }
        );

        synchronized (this) {
            if (hasLatestTemplate.get() == false) {
                // synchronized so that hasLatestTemplate does not change value
                // between the read and adding to the backlog
                assert backlog != null;
                if (backlog != null) {
                    if (backlog.size() >= MAX_BUFFER_SIZE) {
                        backlog.remove();
                    }
                    backlog.add(toXContent);
                } else {
                    logger.error("Latest audit template missing but the back log has been written");
                }

                // stop multiple invocations
                if (putTemplateInProgress.compareAndSet(false, true)) {
                    MlIndexAndAlias.installIndexTemplateIfRequired(clusterService.state(), client, templateSupplier.get(),
                        putTemplateListener);
                }
                return;
            }
        }

        indexDoc(toXContent);
     }

    private void writeDoc(ToXContent toXContent) {
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
        assert backlog != null;
        if (backlog == null) {
            logger.error("Message back log has already been written");
            return;
        }

        BulkRequest bulkRequest = new BulkRequest();
        ToXContent doc = backlog.poll();
        while (doc != null) {
            bulkRequest.add(indexRequest(doc));
            doc = backlog.poll();
        }

        client.bulk(bulkRequest, ActionListener.wrap(
            bulkItemResponses -> {
                if (bulkItemResponses.hasFailures()) {
                    logger.warn("Failures bulk indexing the message back log: {}", bulkItemResponses.buildFailureMessage());
                } else {
                    logger.trace("Successfully wrote audit message backlog after upgrading template");
                }
                backlog = null;
            },
            this::onIndexFailure
        ));
    }

    // for testing
    int backLogSize() {
        return backlog.size();
    }
}
