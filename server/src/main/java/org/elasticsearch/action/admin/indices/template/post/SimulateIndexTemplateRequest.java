/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

public class SimulateIndexTemplateRequest extends MasterNodeReadRequest<SimulateIndexTemplateRequest> {

    private String indexName;

    @Nullable
    private PutComposableIndexTemplateAction.Request indexTemplateRequest;

    private boolean includeDefaults = false;

    public SimulateIndexTemplateRequest(String indexName) {
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException("index name cannot be null or empty");
        }
        this.indexName = indexName;
    }

    public SimulateIndexTemplateRequest(StreamInput in) throws IOException {
        super(in);
        indexName = in.readString();
        indexTemplateRequest = in.readOptionalWriteable(PutComposableIndexTemplateAction.Request::new);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_007)) {
            includeDefaults = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeOptionalWriteable(indexTemplateRequest);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_007)) {
            out.writeBoolean(includeDefaults);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexTemplateRequest != null) {
            validationException = indexTemplateRequest.validateIndexTemplate(validationException);
        }
        return validationException;
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Nullable
    public PutComposableIndexTemplateAction.Request getIndexTemplateRequest() {
        return indexTemplateRequest;
    }

    public SimulateIndexTemplateRequest indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public SimulateIndexTemplateRequest indexTemplateRequest(PutComposableIndexTemplateAction.Request indexTemplateRequest) {
        this.indexTemplateRequest = indexTemplateRequest;
        return this;
    }

    public SimulateIndexTemplateRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimulateIndexTemplateRequest that = (SimulateIndexTemplateRequest) o;
        return indexName.equals(that.indexName)
            && Objects.equals(indexTemplateRequest, that.indexTemplateRequest)
            && includeDefaults == that.includeDefaults;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexTemplateRequest, includeDefaults);
    }
}
