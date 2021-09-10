/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.index;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.TimeValue.timeValueMillis;

public class IndexAction implements Action {

    public static final String TYPE = "index";

    @Nullable final String index;
    @Nullable final String docId;
    @Nullable final DocWriteRequest.OpType opType;
    @Nullable final String executionTimeField;
    @Nullable final TimeValue timeout;
    @Nullable final ZoneId dynamicNameTimeZone;
    @Nullable final RefreshPolicy refreshPolicy;

    public IndexAction(@Nullable String index, @Nullable String docId, @Nullable DocWriteRequest.OpType opType,
                       @Nullable String executionTimeField, @Nullable TimeValue timeout, @Nullable ZoneId dynamicNameTimeZone,
                       @Nullable RefreshPolicy refreshPolicy) {
        this.index = index;
        this.docId = docId;
        this.opType = opType;
        this.executionTimeField = executionTimeField;
        this.timeout = timeout;
        this.dynamicNameTimeZone = dynamicNameTimeZone;
        this.refreshPolicy = refreshPolicy;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public String getIndex() {
        return index;
    }

    public String getDocId() {
        return docId;
    }

    public DocWriteRequest.OpType getOpType() {
        return opType;
    }

    public String getExecutionTimeField() {
        return executionTimeField;
    }

    public ZoneId getDynamicNameTimeZone() {
        return dynamicNameTimeZone;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexAction that = (IndexAction) o;

        return Objects.equals(index, that.index)
                && Objects.equals(docId, that.docId)
                && Objects.equals(opType, that.opType)
                && Objects.equals(executionTimeField, that.executionTimeField)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(dynamicNameTimeZone, that.dynamicNameTimeZone)
                && Objects.equals(refreshPolicy, that.refreshPolicy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, docId, opType, executionTimeField, timeout, dynamicNameTimeZone, refreshPolicy);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (index != null) {
            builder.field(Field.INDEX.getPreferredName(), index);
        }
        if (docId != null) {
            builder.field(Field.DOC_ID.getPreferredName(), docId);
        }
        if (opType != null) {
            builder.field(Field.OP_TYPE.getPreferredName(), opType);
        }
        if (executionTimeField != null) {
            builder.field(Field.EXECUTION_TIME_FIELD.getPreferredName(), executionTimeField);
        }
        if (timeout != null) {
            builder.humanReadableField(Field.TIMEOUT.getPreferredName(), Field.TIMEOUT_HUMAN.getPreferredName(), timeout);
        }
        if (dynamicNameTimeZone != null) {
            builder.field(Field.DYNAMIC_NAME_TIMEZONE.getPreferredName(), dynamicNameTimeZone.toString());
        }
        if (refreshPolicy!= null) {
            builder.field(Field.REFRESH.getPreferredName(), refreshPolicy.getValue());
        }
        return builder.endObject();
    }

    public static IndexAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String index = null;
        String docId = null;
        DocWriteRequest.OpType opType = null;
        String executionTimeField = null;
        TimeValue timeout = null;
        ZoneId dynamicNameTimeZone = null;
        RefreshPolicy refreshPolicy = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    index = parser.text();
                } catch (ElasticsearchParseException pe) {
                    throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. failed to parse index name value for " +
                            "field [{}]", pe, TYPE, watchId, actionId, currentFieldName);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (Field.TIMEOUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    timeout = timeValueMillis(parser.longValue());
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. unexpected number field [{}]", TYPE,
                            watchId, actionId, currentFieldName);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.DOC_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    docId = parser.text();
                } else if (Field.OP_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        opType = DocWriteRequest.OpType.fromString(parser.text());
                        if (List.of(DocWriteRequest.OpType.CREATE, DocWriteRequest.OpType.INDEX).contains(opType) == false) {
                            throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. op_type value for field [{}] " +
                                "must be [index] or [create]", TYPE, watchId, actionId, currentFieldName);
                        }
                    } catch (IllegalArgumentException e) {
                        throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. failed to parse op_type value for " +
                            "field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (Field.EXECUTION_TIME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    executionTimeField = parser.text();
                } else if (Field.TIMEOUT_HUMAN.match(currentFieldName, parser.getDeprecationHandler())) {
                    // Parser for human specified timeouts and 2.x compatibility
                    timeout = WatcherDateTimeUtils.parseTimeValue(parser, Field.TIMEOUT_HUMAN.toString());
                } else if (Field.DYNAMIC_NAME_TIMEZONE.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        dynamicNameTimeZone = DateUtils.of(parser.text());
                    } else {
                        throw new ElasticsearchParseException("could not parse [{}] action for watch [{}]. failed to parse [{}]. must be " +
                                                              "a string value (e.g. 'UTC' or '+01:00').", TYPE, watchId, currentFieldName);
                    }
                } else if (Field.REFRESH.match(currentFieldName, parser.getDeprecationHandler())) {
                    refreshPolicy = RefreshPolicy.parse(parser.text());
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. unexpected string field [{}]", TYPE,
                            watchId, actionId, currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. unexpected token [{}]", TYPE, watchId,
                        actionId, token);
            }
        }

        return new IndexAction(index, docId, opType, executionTimeField, timeout, dynamicNameTimeZone, refreshPolicy);
    }

    public static Builder builder(String index) {
        return new Builder(index);
    }

    public static class Result extends Action.Result {

        private final XContentSource response;

        public Result(Status status, XContentSource response) {
            super(TYPE, status);
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

    static class Simulated extends Action.Result {

        private final String index;
        @Nullable private final String docId;
        @Nullable private final RefreshPolicy refreshPolicy;
        private final XContentSource source;

        protected Simulated(String index, @Nullable String docId, @Nullable RefreshPolicy refreshPolicy,
                            XContentSource source) {
            super(TYPE, Status.SIMULATED);
            this.index = index;
            this.docId = docId;
            this.source = source;
            this.refreshPolicy = refreshPolicy;
        }

        public String index() {
            return index;
        }

        public String docId() {
            return docId;
        }

        public XContentSource source() {
            return source;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(type)
                       .startObject(Field.REQUEST.getPreferredName())
                            .field(Field.INDEX.getPreferredName(), index);

            if (docId != null) {
                builder.field(Field.DOC_ID.getPreferredName(), docId);
            }

            if (refreshPolicy != null) {
                builder.field(Field.REFRESH.getPreferredName(), refreshPolicy.getValue());
            }

            return builder.field(Field.SOURCE.getPreferredName(), source, params)
                       .endObject()
                   .endObject();
        }
    }

    public static class Builder implements Action.Builder<IndexAction> {

        final String index;
        String docId;
        DocWriteRequest.OpType opType;
        String executionTimeField;
        TimeValue timeout;
        ZoneId dynamicNameTimeZone;
        RefreshPolicy refreshPolicy;

        private Builder(String index) {
            this.index = index;
        }

        public Builder setDocId(String docId) {
            this.docId = docId;
            return this;
        }

        public Builder setOpType(DocWriteRequest.OpType opType) {
            this.opType = opType;
            return this;
        }

        public Builder setExecutionTimeField(String executionTimeField) {
            this.executionTimeField = executionTimeField;
            return this;
        }

        public Builder setTimeout(TimeValue writeTimeout) {
            this.timeout = writeTimeout;
            return this;
        }

        public Builder setDynamicNameTimeZone(ZoneId dynamicNameTimeZone) {
            this.dynamicNameTimeZone = dynamicNameTimeZone;
            return this;
        }

        public Builder setRefreshPolicy(RefreshPolicy refreshPolicy) {
            this.refreshPolicy = refreshPolicy;
            return this;
        }

        @Override
        public IndexAction build() {
            return new IndexAction(index, docId, opType, executionTimeField, timeout, dynamicNameTimeZone, refreshPolicy);
        }
    }

    interface Field {
        ParseField INDEX = new ParseField("index");
        ParseField DOC_ID = new ParseField("doc_id");
        ParseField OP_TYPE = new ParseField("op_type");
        ParseField EXECUTION_TIME_FIELD = new ParseField("execution_time_field");
        ParseField SOURCE = new ParseField("source");
        ParseField RESPONSE = new ParseField("response");
        ParseField REQUEST = new ParseField("request");
        ParseField TIMEOUT = new ParseField("timeout_in_millis");
        ParseField TIMEOUT_HUMAN = new ParseField("timeout");
        ParseField DYNAMIC_NAME_TIMEZONE = new ParseField("dynamic_name_timezone");
        ParseField REFRESH = new ParseField("refresh");
    }
}
