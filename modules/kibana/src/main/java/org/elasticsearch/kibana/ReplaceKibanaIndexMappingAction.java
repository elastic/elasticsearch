/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Replaces (rather than merges) the mapping of a Kibana system index. Unlike the public put-mapping API, whose merge
 * semantics are strictly additive, this action installs the submitted mapping as the complete new mapping for the
 * index, allowing Kibana to shed fields it no longer uses ("drop column" semantics).
 * <p>
 * This capability is deliberately restricted to Kibana's saved-objects system indices: they are single-shard,
 * hidden from users, and Kibana is their only reader and writer, so Kibana can guarantee at the application level
 * that a dropped field is no longer written or queried, and that its values have been purged from {@code _source}
 * before the mapping is dropped.
 */
public final class ReplaceKibanaIndexMappingAction {

    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>("indices:admin/kibana/replace_mapping");

    private ReplaceKibanaIndexMappingAction() {}

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String index;
        private final String mappingSource;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String index, String mappingSource) {
            super(masterNodeTimeout, ackTimeout);
            this.index = index;
            this.mappingSource = mappingSource;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
            this.mappingSource = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(mappingSource);
        }

        public String index() {
            return index;
        }

        public String mappingSource() {
            return mappingSource;
        }

        @Override
        public String[] indices() {
            return new String[] { index };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(index) == false) {
                validationException = ValidateActions.addValidationError("index is required", validationException);
            }
            if (Strings.hasText(mappingSource) == false) {
                validationException = ValidateActions.addValidationError("mapping source is required", validationException);
            }
            return validationException;
        }
    }
}
