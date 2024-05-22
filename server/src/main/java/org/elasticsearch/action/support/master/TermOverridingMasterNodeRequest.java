/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Wrapper around a {@link MasterNodeRequest} for use when sending the request to another node, overriding the {@link
 * MasterNodeRequest#masterTerm()} sent out over the wire.
 */
public class TermOverridingMasterNodeRequest extends TransportRequest {

    private static final Logger logger = LogManager.getLogger(TermOverridingMasterNodeRequest.class);

    private final MasterNodeRequest<?> request;
    private final long newMasterTerm;

    TermOverridingMasterNodeRequest(MasterNodeRequest<?> request, long newMasterTerm) {
        assert request.masterTerm() <= newMasterTerm;
        this.request = request;
        this.newMasterTerm = newMasterTerm;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return unsupported();
    }

    @Override
    public String getDescription() {
        return request.getDescription();
    }

    @Override
    public void remoteAddress(InetSocketAddress remoteAddress) {
        unsupported();
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return unsupported();
    }

    @Override
    public void incRef() {
        request.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return request.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return request.decRef();
    }

    @Override
    public boolean hasReferences() {
        return request.hasReferences();
    }

    @Override
    public void setParentTask(String parentTaskNode, long parentTaskId) {
        unsupported();
    }

    @Override
    public void setParentTask(TaskId taskId) {
        unsupported();
    }

    @Override
    public TaskId getParentTask() {
        return request.getParentTask();
    }

    @Override
    public void setRequestId(long requestId) {
        request.setRequestId(requestId);
    }

    @Override
    public long getRequestId() {
        return request.getRequestId();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        request.writeTo(new TermOverridingStreamOutput(out, newMasterTerm));
    }

    @Override
    public String toString() {
        return Strings.format("TermOverridingMasterNodeRequest[newMasterTerm={} in {}]", newMasterTerm, request);
    }

    private static <T> T unsupported() {
        final var exception = new UnsupportedOperationException("TermOverridingMasterNodeRequest is only for outbound requests");
        logger.error("TermOverridingMasterNodeRequest is only for outbound requests", exception);
        assert false : exception;
        throw exception;
    }
}
