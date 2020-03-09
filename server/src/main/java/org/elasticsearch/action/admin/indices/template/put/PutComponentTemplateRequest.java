/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request for putting a single component template into the cluster state
 */
public class PutComponentTemplateRequest extends MasterNodeRequest<PutComponentTemplateRequest> {
    private final String name;
    @Nullable
    private String cause;
    private boolean create;
    private ComponentTemplate componentTemplate;

    public PutComponentTemplateRequest(StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        this.cause = in.readOptionalString();
        this.create = in.readBoolean();
        this.componentTemplate = new ComponentTemplate(in);
    }

    /**
     * Constructs a new put component template request with the provided name.
     */
    public PutComponentTemplateRequest(String name) {
        this.name = name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeOptionalString(cause);
        out.writeBoolean(create);
        this.componentTemplate.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (componentTemplate == null) {
            validationException = addValidationError("a component template is required", validationException);
        }
        return validationException;
    }

    /**
     * The name of the index template.
     */
    public String name() {
        return this.name;
    }

    /**
     * Set to {@code true} to force only creation, not an update of an index template. If it already
     * exists, it will fail with an {@link IllegalArgumentException}.
     */
    public PutComponentTemplateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public boolean create() {
        return create;
    }

    /**
     * The cause for this index template creation.
     */
    public PutComponentTemplateRequest cause(@Nullable String cause) {
        this.cause = cause;
        return this;
    }

    @Nullable
    public String cause() {
        return this.cause;
    }

    /**
     * The component template that will be inserted into the cluster state
     */
    public PutComponentTemplateRequest componentTemplate(ComponentTemplate template) {
        this.componentTemplate = template;
        return this;
    }

    public ComponentTemplate componentTemplate() {
        return this.componentTemplate;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PutComponentRequest[");
        sb.append("name=").append(name);
        sb.append(", cause=").append(cause);
        sb.append(", create=").append(create);
        sb.append(", component_template=").append(componentTemplate);
        sb.append("]");
        return sb.toString();
    }
}
