/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractAuditor<T extends AbstractAuditMessage> {

    // The special ID that means the message applies to all jobs/resources.
    public static final String All_RESOURCES_ID = "";

    private static final Logger logger = LogManager.getLogger(AbstractAuditor.class);
    static final int MAX_BUFFER_SIZE = 1000;
    static final TimeValue MASTER_TIMEOUT = TimeValue.timeValueMinutes(1);

    private final OriginSettingClient client;
    private final String nodeName;
    private final String auditIndex;
    private final String templateName;
    private final Supplier<PutComposableIndexTemplateAction.Request> templateSupplier;
    private final AbstractAuditMessageFactory<T> messageFactory;
    private final AtomicBoolean hasLatestTemplate;

    private Queue<ToXContent> backlog;
    private final ClusterService clusterService;
    private final AtomicBoolean putTemplateInProgress;

    protected AbstractAuditor(
        OriginSettingClient client,
        String auditIndex,
        IndexTemplateConfig templateConfig,
        String nodeName,
        AbstractAuditMessageFactory<T> messageFactory,
        ClusterService clusterService
    ) {

        this(client, auditIndex, templateConfig.getTemplateName(), () -> {
            try {
                return new PutComposableIndexTemplateAction.Request(templateConfig.getTemplateName()).indexTemplate(
                    ComposableIndexTemplate.parse(
                        JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, templateConfig.loadBytes())
                    )
                ).masterNodeTimeout(MASTER_TIMEOUT);
            } catch (IOException e) {
                throw new ElasticsearchParseException("unable to parse composable template " + templateConfig.getTemplateName(), e);
            }
        }, nodeName, messageFactory, clusterService);
    }

    protected AbstractAuditor(
        OriginSettingClient client,
        String auditIndex,
        String templateName,
        Supplier<PutComposableIndexTemplateAction.Request> templateSupplier,
        String nodeName,
        AbstractAuditMessageFactory<T> messageFactory,
        ClusterService clusterService
    ) {
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

    private static void onIndexResponse(IndexResponse response) {
        logger.trace("Successfully wrote audit message");
    }

    private static void onIndexFailure(Exception exception) {
        logger.debug("Failed to write audit message", exception);
    }

    protected void indexDoc(ToXContent toXContent) {
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

        ActionListener<Boolean> putTemplateListener = ActionListener.wrap(r -> {
            synchronized (this) {
                // synchronized so nothing can be added to backlog while this value changes
                hasLatestTemplate.set(true);
            }
            logger.info("Auditor template [{}] successfully installed", templateName);
            writeBacklog();
            putTemplateInProgress.set(false);
        }, e -> {
            logger.warn("Error putting latest template [{}]", templateName);
            putTemplateInProgress.set(false);
        });

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
                    MlIndexAndAlias.installIndexTemplateIfRequired(
                        clusterService.state(),
                        client,
                        templateSupplier.get(),
                        putTemplateListener
                    );
                }
                return;
            }
        }

        indexDoc(toXContent);
    }

    private void writeDoc(ToXContent toXContent) {
        client.index(indexRequest(toXContent), ActionListener.wrap(AbstractAuditor::onIndexResponse, AbstractAuditor::onIndexFailure));
    }

    private IndexRequest indexRequest(ToXContent toXContent) {
        IndexRequest indexRequest = new IndexRequest(auditIndex);
        indexRequest.source(toXContentBuilder(toXContent));
        indexRequest.timeout(TimeValue.timeValueSeconds(5));
        return indexRequest;
    }

    private static XContentBuilder toXContentBuilder(ToXContent toXContent) {
        try (XContentBuilder jsonBuilder = jsonBuilder()) {
            return toXContent.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void clearBacklog() {
        backlog = null;
    }

    protected void writeBacklog() {
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

        client.bulk(bulkRequest, ActionListener.wrap(bulkItemResponses -> {
            if (bulkItemResponses.hasFailures()) {
                logger.warn("Failures bulk indexing the message back log: {}", bulkItemResponses.buildFailureMessage());
            } else {
                logger.trace("Successfully wrote audit message backlog after upgrading template");
            }
            backlog = null;
        }, AbstractAuditor::onIndexFailure));
    }

    // for testing
    int backLogSize() {
        return backlog.size();
    }
}
