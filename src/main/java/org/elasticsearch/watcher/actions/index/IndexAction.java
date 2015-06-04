/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.support.xcontent.XContentSource;

import java.io.IOException;

/**
 *
 */
public class IndexAction implements Action {

    public static final String TYPE = "index";

    final String index;
    final String docType;
    final @Nullable String executionTimeField;

    public IndexAction(String index, String docType, @Nullable String executionTimeField) {
        this.index = index;
        this.docType = docType;
        this.executionTimeField = executionTimeField;
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

    public String getExecutionTimeField() {
        return executionTimeField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexAction that = (IndexAction) o;

        if (!index.equals(that.index)) return false;
        if (!docType.equals(that.docType)) return false;
        return !(executionTimeField != null ? !executionTimeField.equals(that.executionTimeField) : that.executionTimeField != null);
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + docType.hashCode();
        result = 31 * result + (executionTimeField != null ? executionTimeField.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.INDEX.getPreferredName(), index);
        builder.field(Field.DOC_TYPE.getPreferredName(), docType);
        if (executionTimeField != null) {
            builder.field(Field.EXECUTION_TIME_FIELD.getPreferredName(), executionTimeField);
        }
        return builder.endObject();
    }

    public static IndexAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String index = null;
        String docType = null;
        String executionTimeField = null;

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
                } else if (Field.EXECUTION_TIME_FIELD.match(currentFieldName)) {
                    executionTimeField = parser.text();
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

        return new IndexAction(index, docType, executionTimeField);
    }

    public static Builder builder(String index, String docType) {
        return new Builder(index, docType);
    }

    public interface Result {

        class Success extends Action.Result implements Result {

            private final XContentSource response;

            public Success(XContentSource response) {
                super(TYPE, Status.SUCCESS);
                this.response = response;
            }

            public XContentSource response() {
                return response;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type)
                        .field(Field.RESPONSE.getPreferredName(), response, params)
                        .endObject();
            }
        }

        class Simulated extends Action.Result implements Result {

            private final String index;
            private final String docType;
            private final XContentSource source;

            protected Simulated(String index, String docType, XContentSource source) {
                super(TYPE, Status.SIMULATED);
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

            public XContentSource source() {
                return source;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type)
                        .startObject(Field.REQUEST.getPreferredName())
                            .field(Field.INDEX.getPreferredName(), index)
                            .field(Field.DOC_TYPE.getPreferredName(), docType)
                            .field(Field.SOURCE.getPreferredName(), source, params)
                        .endObject()
                        .endObject();
            }
        }
    }

    public static class Builder implements Action.Builder<IndexAction> {

        final String index;
        final String docType;
        String executionTimeField;

        private Builder(String index, String docType) {
            this.index = index;
            this.docType = docType;
        }

        public Builder setExecutionTimeField(String executionTimeField) {
            this.executionTimeField = executionTimeField;
            return this;
        }

        @Override
        public IndexAction build() {
            return new IndexAction(index, docType, executionTimeField);
        }
    }

    interface Field extends Action.Field {
        ParseField INDEX = new ParseField("index");
        ParseField DOC_TYPE = new ParseField("doc_type");
        ParseField EXECUTION_TIME_FIELD = new ParseField("execution_time_field");
        ParseField SOURCE = new ParseField("source");
        ParseField RESPONSE = new ParseField("response");
        ParseField REQUEST = new ParseField("request");
    }
}
