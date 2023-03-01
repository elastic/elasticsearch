/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.entsearch.engine.Engine;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteEngineAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteEngineAction INSTANCE = new DeleteEngineAction();
    public static final String NAME = "cluster:admin/engine/delete";

    private DeleteEngineAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest implements IndicesRequest {

        private final String[] names;
        private final IndicesOptions indicesOptions = Engine.INDICES_OPTIONS;

        private final String engineId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.engineId = in.readString();
            names = new String[] { Engine.getEngineAliasName(this.engineId) };
        }

        public Request(String engineId) {
            this.engineId = engineId;
            names = new String[] { Engine.getEngineAliasName(engineId) };
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (engineId == null || engineId.isEmpty()) {
                validationException = addValidationError("engineId missing", validationException);
            }

            return validationException;
        }

        public String getEngineId() {
            return engineId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(engineId);
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(engineId, that.engineId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(engineId);
        }
    }

}
