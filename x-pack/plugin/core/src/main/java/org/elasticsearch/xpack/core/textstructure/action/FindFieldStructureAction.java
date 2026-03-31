/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindFieldStructureAction extends ActionType<FindStructureResponse> {

    public static final FindFieldStructureAction INSTANCE = new FindFieldStructureAction();
    public static final String NAME = "cluster:monitor/text_structure/find_field_structure";

    private FindFieldStructureAction() {
        super(NAME);
    }

    public static class Request extends AbstractFindStructureRequest {

        public static final ParseField INDEX = new ParseField("index");
        public static final ParseField FIELD = new ParseField("field");

        private String index;
        private String field;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
            field = in.readString();
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = super.validate();
            if (Strings.isNullOrEmpty(index)) {
                validationException = addValidationError("index must be specified", validationException);
            }
            if (Strings.isNullOrEmpty(field)) {
                validationException = addValidationError("field must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), field, index);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Request that = (Request) other;
            return super.equals(other) && Objects.equals(this.index, that.index) && Objects.equals(this.field, that.field);
        }
    }
}
