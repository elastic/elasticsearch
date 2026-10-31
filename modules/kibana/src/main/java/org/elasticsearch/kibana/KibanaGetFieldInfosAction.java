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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

/**
 * Internal action that reads Lucene {@link org.apache.lucene.index.FieldInfos} field names from a shard of a Kibana
 * system index. Used as a pre-flight step in {@link TransportReplaceKibanaIndexMappingAction} to detect field names
 * that Lucene has permanently committed and would conflict with net-new fields in an incoming mapping replacement.
 */
final class KibanaGetFieldInfosAction {

    static final String NAME = "internal:kibana/get_field_infos";
    static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private KibanaGetFieldInfosAction() {}

    static final class Request extends SingleShardRequest<Request> {

        Request(String index) {
            this.index(index);
        }

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return validateNonNullIndex();
        }
    }

    static final class Response extends ActionResponse {

        private final Set<String> fieldNames;

        Response(Set<String> fieldNames) {
            this.fieldNames = fieldNames;
        }

        Response(StreamInput in) throws IOException {
            fieldNames = in.readCollectionAsImmutableSet(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(fieldNames);
        }

        Set<String> fieldNames() {
            return fieldNames;
        }
    }
}
