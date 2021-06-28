/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * An action for putting a single component template into the cluster state
 */
public class PutComponentTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final PutComponentTemplateAction INSTANCE = new PutComponentTemplateAction();
    public static final String NAME = "cluster:admin/component_template/put";

    private PutComponentTemplateAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    /**
     * A request for putting a single component template into the cluster state
     */
    public static class Request extends MasterNodeRequest<Request> {
        private final String name;
        @Nullable
        private String cause;
        private boolean create;
        private ComponentTemplate componentTemplate;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.cause = in.readOptionalString();
            this.create = in.readBoolean();
            this.componentTemplate = new ComponentTemplate(in);
        }

        /**
         * Constructs a new put component template request with the provided name.
         */
        public Request(String name) {
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
            if (name == null || Strings.hasText(name) == false) {
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
        public Request create(boolean create) {
            this.create = create;
            return this;
        }

        public boolean create() {
            return create;
        }

        /**
         * The cause for this index template creation.
         */
        public Request cause(@Nullable String cause) {
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
        public Request componentTemplate(ComponentTemplate template) {
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

}
