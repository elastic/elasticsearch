/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.audit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.audit.InferenceAuditEventDoc;

import java.io.IOException;

/**
 * Writes best-effort, fire-and-forget audit entries to the {@code .audit-inference} system data
 * stream whenever inference resources are created, updated, or deleted. All audit writes are
 * asynchronous: failures are logged at WARN level and never propagate back to the caller, so the
 * audit trail never interferes with the primary operation being audited.
 */
public class InferenceAuditService {

    private static final Logger logger = LogManager.getLogger(InferenceAuditService.class);

    public static final String DATA_STREAM_NAME = ".audit-inference";

    private final OriginSettingClient client;

    public InferenceAuditService(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    /**
     * Writes a best-effort, fire-and-forget audit entry for a generic audit event (e.g. create,
     * update, or delete). Audit write failures are logged as warnings but do not affect the
     * caller's response. If {@link InferenceAuditEventDoc#resource()} is non-null its XContent
     * representation is nested under the {@code resource} field.
     */
    public void auditEvent(InferenceAuditEventDoc event) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            event.toXContent(builder, ToXContent.EMPTY_PARAMS);
            client.prepareIndex(DATA_STREAM_NAME)
                .setSource(builder)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
                .execute(loggingListener(event.action() + " " + event.resourceType()));
        } catch (IOException e) {
            logger.warn("Failed to serialize audit entry for [{}] [{}]", event.action(), event.resourceType(), e);
        }
    }

    private static ActionListener<DocWriteResponse> loggingListener(String operation) {
        return ActionListener.wrap(
            response -> logger.debug("Audit entry written for [{}]", operation),
            e -> logger.warn("Failed to write audit entry for [{}]", operation, e)
        );
    }
}
