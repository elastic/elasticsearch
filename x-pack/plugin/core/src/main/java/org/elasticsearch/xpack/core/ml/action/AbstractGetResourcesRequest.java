/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractGetResourcesRequest extends ActionRequest {

    private String resourceId;
    private PageParams pageParams = PageParams.defaultParams();

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        resourceId = in.readOptionalString();
        pageParams = in.readOptionalWriteable(PageParams::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(resourceId);
        out.writeOptionalWriteable(pageParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceId, pageParams);
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
        return Objects.equals(resourceId, other.resourceId);
    }

    public abstract String getResourceIdField();
}
