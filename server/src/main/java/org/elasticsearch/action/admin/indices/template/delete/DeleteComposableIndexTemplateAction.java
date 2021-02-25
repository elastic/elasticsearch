/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteComposableIndexTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteComposableIndexTemplateAction INSTANCE = new DeleteComposableIndexTemplateAction();
    public static final String NAME = "indices:admin/index_template/delete";

    private DeleteComposableIndexTemplateAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String name;

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        public Request() { }

        /**
         * Constructs a new delete template request for the specified name.
         */
        public Request(String name) {
            this.name = name;
        }

        /**
         * Set the index template name to delete.
         */
        public Request name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null) {
                validationException = addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        /**
         * The index template name to delete.
         */
        public String name() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(other.name, this.name);
        }
    }
}
