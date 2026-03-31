/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindMessageStructureAction extends ActionType<FindStructureResponse> {

    public static final FindMessageStructureAction INSTANCE = new FindMessageStructureAction();
    public static final String NAME = "cluster:monitor/text_structure/find_message_structure";

    private FindMessageStructureAction() {
        super(NAME);
    }

    public static class Request extends AbstractFindStructureRequest {

        public static final ParseField MESSAGES = new ParseField("messages");

        private List<String> messages;

        private static final ObjectParser<Request, Void> PARSER = createParser();

        private static ObjectParser<Request, Void> createParser() {
            ObjectParser<Request, Void> parser = new ObjectParser<>("text_structure/find_message_structure", false, Request::new);
            parser.declareStringArray(Request::setMessages, MESSAGES);
            return parser;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            messages = in.readStringCollectionAsList();
        }

        public static Request parseRequest(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public List<String> getMessages() {
            return messages;
        }

        public void setMessages(List<String> messages) {
            this.messages = messages;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = super.validate();
            if (messages == null || messages.isEmpty()) {
                validationException = addValidationError("messages must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(messages);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), messages);
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
            return super.equals(other) && Objects.equals(this.messages, that.messages);
        }
    }
}
