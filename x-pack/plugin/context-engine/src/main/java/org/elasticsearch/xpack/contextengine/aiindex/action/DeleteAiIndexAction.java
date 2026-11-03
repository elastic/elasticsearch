/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeleteAiIndexAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/ai_index/delete";
    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>(NAME);

    private DeleteAiIndexAction() {/* no instances */}

    public static class Request extends UntypedActionRequest {

        private final String space;
        private final String id;

        public Request(String space, String id) {
            this.space = space;
            this.id = id;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.space = in.readString();
            this.id = in.readString();
        }

        public String space() {
            return space;
        }

        public String id() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(space);
            out.writeString(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return GetAiIndexAction.Request.validateSpaceAndId(space, id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(space, request.space) && Objects.equals(id, request.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(space, id);
        }
    }
}
