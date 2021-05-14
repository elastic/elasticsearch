/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteComposableIndexTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteComposableIndexTemplateAction INSTANCE = new DeleteComposableIndexTemplateAction();
    public static final String NAME = "indices:admin/index_template/delete";

    private DeleteComposableIndexTemplateAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String[] names;

        public Request(StreamInput in) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(Version.V_7_13_0)) {
                names = in.readStringArray();
            } else {
                names = new String[] {in.readString()};
            }
        }

        /**
         * Constructs a new delete template request for the specified name.
         */
        public Request(String... names) {
            this.names = Objects.requireNonNull(names, "templates to delete must not be null");
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Arrays.stream(names).anyMatch(Strings::hasLength) == false) {
                validationException = addValidationError("no template names specified", validationException);
            }
            return validationException;
        }

        /**
         * The index template names to delete.
         */
        public String[] names() {
            return names;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
                out.writeStringArray(names);
            } else {
                out.writeString(names[0]);
            }
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
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
            return Arrays.equals(other.names, this.names);
        }
    }
}
