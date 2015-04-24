/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class IndexAction implements Action {

    public static final String TYPE = "index";

    final String index;
    final String docType;

    public IndexAction(String index, String docType) {
        this.index = index;
        this.docType = docType;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public String getIndex() {
        return index;
    }

    public String getDocType() {
        return docType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexAction action = (IndexAction) o;

        if (!index.equals(action.index)) return false;
        return docType.equals(action.docType);
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + docType.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(Field.INDEX.getPreferredName(), index)
                .field(Field.DOC_TYPE.getPreferredName(), docType)
                .endObject();
    }

    public static IndexAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String index = null;
        String docType = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.INDEX.match(currentFieldName)) {
                    index = parser.text();
                } else if (Field.DOC_TYPE.match(currentFieldName)) {
                    docType = parser.text();
                } else {
                    throw new IndexActionException("could not parse [{}] action [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                }
            } else {
                throw new IndexActionException("could not parse [{}] action [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
            }
        }

        if (index == null) {
            throw new IndexActionException("could not parse [{}] action [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.INDEX.getPreferredName());
        }

        if (docType == null) {
            throw new IndexActionException("could not parse [{}] action [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.DOC_TYPE.getPreferredName());
        }

        return new IndexAction(index, docType);
    }

    public static Builder builder(String index, String docType) {
        return new Builder(index, docType);
    }

    public abstract static class Result extends Action.Result {

        protected Result(boolean success) {
            super(TYPE, success);
        }

        static class Executed extends Result {

            private final Payload response;

            public Executed(Payload response, boolean isCreated) {
                super(isCreated);
                this.response = response;
            }

            public Payload response() {
                return response;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                if (response != null) {
                    builder.field(Field.RESPONSE.getPreferredName(), response, params);
                }
                return builder;
            }
        }

        static class Failure extends Result {

            private final String reason;

            public Failure(String reason) {
                super(false);
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REASON.getPreferredName(), reason);
            }
        }

        public static class Simulated extends Result {

            private final String index;
            private final String docType;
            private final Payload source;

            protected Simulated(String index, String docType, Payload source) {
                super(true);
                this.index = index;
                this.docType = docType;
                this.source = source;
            }

            public String index() {
                return index;
            }

            public String docType() {
                return docType;
            }

            public Payload source() {
                return source;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(Field.SIMULATED_REQUEST.getPreferredName())
                        .field(Field.INDEX.getPreferredName(), index)
                        .field(Field.DOC_TYPE.getPreferredName(), docType)
                        .field(Field.SOURCE.getPreferredName(), source, params)
                        .endObject();
            }
        }

        public static Result parse(String watchId, String actionId, XContentParser parser) throws IOException {
            Boolean success = null;
            Payload response = null;
            String reason = null;
            Payload source = null;
            String index = null;
            String docType = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (Field.REASON.match(currentFieldName)) {
                        reason = parser.text();
                    } else {
                        throw new IndexActionException("could not parse [{}] action result [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (Field.SUCCESS.match(currentFieldName)) {
                        success = parser.booleanValue();
                    } else {
                        throw new IndexActionException("could not parse [{}] action result [{}/{}]. unexpected boolean field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Field.RESPONSE.match(currentFieldName)) {
                        response = new Payload.Simple(parser.map());
                    } else if (Field.SIMULATED_REQUEST.match(currentFieldName)) {
                        String context = currentFieldName;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if (Field.INDEX.match(currentFieldName)) {
                                    index = parser.text();
                                } else if (Field.DOC_TYPE.match(currentFieldName)) {
                                    docType = parser.text();
                                } else {
                                    throw new IndexActionException("could not parse [{}] action result [{}/{}]. unexpected string field [{}.{}]", TYPE, watchId, actionId, context, currentFieldName);
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                if (Field.SOURCE.match(currentFieldName)) {
                                    source = new Payload.Simple(parser.map());
                                } else {
                                    throw new IndexActionException("could not parse [{}] action result [{}/{}]. unexpected object field [{}.{}]", TYPE, watchId, actionId, context, currentFieldName);
                                }
                            }
                        }
                    } else {
                        throw new IndexActionException("could not parse [{}] action result [{}/{}]. unexpected object field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else {
                    throw new IndexActionException("could not parse [{}] action result [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
                }
            }

            if (index != null || docType != null || source != null) {
                assertNotNull(index, "could not parse simulated [{}] action result [{}/{}]. missing required [{}.{}] field", TYPE, watchId, actionId, Field.SIMULATED_REQUEST.getPreferredName(), Field.INDEX.getPreferredName());
                assertNotNull(docType, "could not parse simulated [{}] action result [{}/{}]. missing required [{}.{}] field", TYPE, watchId, actionId, Field.SIMULATED_REQUEST.getPreferredName(), Field.DOC_TYPE.getPreferredName());
                assertNotNull(source, "could not parse simulated [{}] action result [{}/{}]. missing required [{}.{}] field", TYPE, watchId, actionId, Field.SIMULATED_REQUEST.getPreferredName(), Field.SOURCE.getPreferredName());
                return new Simulated(index, docType, source);
            }

            assertNotNull(success, "could not parse [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.SUCCESS.getPreferredName());

            if (reason != null) {
                return new Failure(reason);
            }

            assertNotNull(response, "could not parse executed [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.RESPONSE.getPreferredName());
            return new Executed(response, success);
        }

        private static void assertNotNull(Object value, String message, Object... args) {
            if (value == null) {
                throw new IndexActionException(message, args);
            }
        }
    }

    public static class Builder implements Action.Builder<IndexAction> {

        final String index;
        final String docType;

        private Builder(String index, String docType) {
            this.index = index;
            this.docType = docType;
        }

        @Override
        public IndexAction build() {
            return new IndexAction(index, docType);
        }
    }

    interface Field extends Action.Field {
        ParseField INDEX = new ParseField("index");
        ParseField DOC_TYPE = new ParseField("doc_type");
        ParseField SOURCE = new ParseField("source");
        ParseField RESPONSE = new ParseField("response");
        ParseField SIMULATED_REQUEST = new ParseField("simulated_request");
    }
}
