/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Renames an index in place. See {@link org.elasticsearch.action.admin.indices.rename.TransportRenameIndexAction} for the semantics.
 */
public class RenameIndexAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "indices:admin/rename";
    public static final RenameIndexAction INSTANCE = new RenameIndexAction();

    public RenameIndexAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {
        private final String sourceIndex;
        private final String destinationIndex;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String sourceIndex, String destinationIndex) {
            super(masterNodeTimeout, ackTimeout);
            this.sourceIndex = sourceIndex;
            this.destinationIndex = destinationIndex;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.destinationIndex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeString(destinationIndex);
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getDestinationIndex() {
            return destinationIndex;
        }

        /**
         * Both names are reported so that security requires the caller to be allowed to manage the index under its old name and
         * under its new name. The destination does not exist yet, which is fine: index names in a non-replaceable request are
         * authorized by name, as they are for index creation.
         */
        @Override
        public String[] indices() {
            return new String[] { sourceIndex, destinationIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(sourceIndex) == false) {
                validationException = ValidateActions.addValidationError("source index is missing", validationException);
            }
            if (Strings.hasText(destinationIndex) == false) {
                validationException = ValidateActions.addValidationError("destination index is missing", validationException);
            }
            if (sourceIndex != null && sourceIndex.equals(destinationIndex)) {
                validationException = ValidateActions.addValidationError(
                    "source and destination index names must differ",
                    validationException
                );
            }
            return validationException;
        }
    }
}
