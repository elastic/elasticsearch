/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractAuditor<T extends AbstractAuditMessage> {

    // The special ID that means the message applies to all jobs/resources.
    public static final String All_RESOURCES_ID = "";

    private static final Logger logger = LogManager.getLogger(AbstractAuditor.class);
    static final int MAX_BUFFER_SIZE = 1000;
    protected static final TimeValue MASTER_TIMEOUT = TimeValue.timeValueMinutes(1);

    private final OriginSettingClient client;
    private final String nodeName;
    private final String auditIndexWriteAlias;
    private final AbstractAuditMessageFactory<T> messageFactory;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AtomicBoolean indexAndAliasCreated;

    private Queue<ToXContent> backlog;
    private final AtomicBoolean indexAndAliasCreationInProgress;
    private final ExecutorService executorService;

    protected AbstractAuditor(
        OriginSettingClient client,
        String auditIndexWriteAlias,
        String nodeName,
        AbstractAuditMessageFactory<T> messageFactory,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ExecutorService executorService
    ) {
        this.client = Objects.requireNonNull(client);
        this.auditIndexWriteAlias = Objects.requireNonNull(auditIndexWriteAlias);
        this.messageFactory = Objects.requireNonNull(messageFactory);
        this.nodeName = Objects.requireNonNull(nodeName);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
        this.backlog = new ConcurrentLinkedQueue<>();
        this.indexAndAliasCreated = new AtomicBoolean();
        this.indexAndAliasCreationInProgress = new AtomicBoolean();
        this.executorService = executorService;
    }

    public void audit(Level level, String resourceId, String message) {
        indexDoc(messageFactory.newMessage(resourceId, message, level, new Date(), nodeName));
    }

    public void info(String resourceId, String message) {
        audit(Level.INFO, resourceId, message);
    }

    public void warning(String resourceId, String message) {
        audit(Level.WARNING, resourceId, message);
    }

    public void error(String resourceId, String message) {
        audit(Level.ERROR, resourceId, message);
    }

    /**
     * Calling reset will cause the auditor to check the required
     * index and alias exist and recreate if necessary
     */
    public void reset() {
        indexAndAliasCreated.set(false);
        // create a new backlog in case documents need
        // to be temporarily stored when the new index/alias is created
        backlog = new ConcurrentLinkedQueue<>();
    }

    private static void onIndexResponse(DocWriteResponse response) {
        logger.trace("Successfully wrote audit message");
    }

    private static void onIndexFailure(Exception exception) {
        logger.debug("Failed to write audit message", exception);
    }

    protected void indexDoc(ToXContent toXContent) {
        if (indexAndAliasCreated.get()) {
            writeDoc(toXContent);
            return;
        }

        // install template & create index with alias
        var createListener = ActionListener.<Boolean>wrap(success -> {
            indexAndAliasCreationInProgress.set(false);
            synchronized (this) {
                // synchronized so nothing can be added to backlog while writing it
                indexAndAliasCreated.set(true);
                writeBacklog();
            }

        }, e -> { indexAndAliasCreationInProgress.set(false); });

        synchronized (this) {
            if (indexAndAliasCreated.get() == false) {
                // synchronized so that hasLatestTemplate does not change value
                // between the read and adding to the backlog
                if (backlog != null) {
                    if (backlog.size() >= MAX_BUFFER_SIZE) {
                        backlog.remove();
                    }
                    backlog.add(toXContent);
                } else {
                    logger.error("Audit message cannot be added to the backlog");
                }

                // stop multiple invocations
                if (indexAndAliasCreationInProgress.compareAndSet(false, true)) {
                    installTemplateAndCreateIndex(createListener);
                }
            }
        }
    }

    private void writeDoc(ToXContent toXContent) {
        client.index(indexRequest(toXContent), ActionListener.wrap(AbstractAuditor::onIndexResponse, e -> {
            if (e instanceof IndexNotFoundException) {
                executorService.execute(() -> {
                    reset();
                    indexDoc(toXContent);
                });
            } else {
                onIndexFailure(e);
            }
        }));
    }

    private IndexRequest indexRequest(ToXContent toXContent) {
        IndexRequest indexRequest = new IndexRequest(auditIndexWriteAlias);
        indexRequest.source(toXContentBuilder(toXContent));
        indexRequest.timeout(TimeValue.timeValueSeconds(5));
        indexRequest.setRequireAlias(true);
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
        if (backlog == null) {
            logger.debug("Message back log has already been written");
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
                logger.trace("Successfully wrote audit message backlog");
            }
            backlog = null;
        }, AbstractAuditor::onIndexFailure));
    }

    // for testing
    int backLogSize() {
        return backlog.size();
    }

    private void installTemplateAndCreateIndex(ActionListener<Boolean> listener) {
        SubscribableListener.<Boolean>newForked(l -> {
            MlIndexAndAlias.installIndexTemplateIfRequired(clusterService.state(), client, templateVersion(), putTemplateRequest(), l);
        }).<Boolean>andThen((l, success) -> {
            var indexDetails = indexDetails();
            MlIndexAndAlias.createIndexAndAliasIfNecessary(
                client,
                clusterService.state(),
                indexNameExpressionResolver,
                indexDetails.indexPrefix(),
                indexDetails.indexVersion(),
                auditIndexWriteAlias,
                MASTER_TIMEOUT,
                ActiveShardCount.DEFAULT,
                l
            );

        }).addListener(listener);
    }

    protected abstract TransportPutComposableIndexTemplateAction.Request putTemplateRequest();

    protected abstract int templateVersion();

    protected abstract IndexDetails indexDetails();

    public record IndexDetails(String indexPrefix, String indexVersion) {};
}
