/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

public class RenameIndexAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "indices:admin/rename";
    public static final RenameIndexAction INSTANCE = new RenameIndexAction();

    public RenameIndexAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
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
    }
}
