/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractGetResourcesRequest extends ActionRequest {

    private String resourceId;
    private PageParams pageParams = PageParams.defaultParams();
    private boolean allowNoResources = false;

    public AbstractGetResourcesRequest() {
    }

    public AbstractGetResourcesRequest(StreamInput in) throws IOException {
        super(in);
        resourceId = in.readOptionalString();
        pageParams = in.readOptionalWriteable(PageParams::new);
        allowNoResources = in.readBoolean();
    }

    // Allow child classes to provide their own defaults if necessary
    protected AbstractGetResourcesRequest(String resourceId, PageParams pageParams, boolean allowNoResources) {
        this.resourceId = resourceId;
        this.pageParams = pageParams;
        this.allowNoResources = allowNoResources;
    }

    public final void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public final String getResourceId() {
        return resourceId;
    }

    public final void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public final PageParams getPageParams() {
        return pageParams;
    }

    public final void setAllowNoResources(boolean allowNoResources) {
        this.allowNoResources = allowNoResources;
    }

    public final boolean isAllowNoResources() {
        return this.allowNoResources;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(resourceId);
        out.writeOptionalWriteable(pageParams);
        out.writeBoolean(allowNoResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceId, pageParams, allowNoResources);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof AbstractGetResourcesRequest == false) {
            return false;
        }
        AbstractGetResourcesRequest other = (AbstractGetResourcesRequest) obj;
        return Objects.equals(resourceId, other.resourceId) &&
            Objects.equals(pageParams, other.pageParams) &&
            allowNoResources == other.allowNoResources;
    }

    public abstract String getResourceIdField();
}
