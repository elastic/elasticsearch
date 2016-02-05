/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 *
 */
public class IndexAction implements Action {

    public static final String TYPE = "index";

    final String index;
    final String docType;
    final @Nullable String executionTimeField;
    final @Nullable TimeValue timeout;
    final @Nullable DateTimeZone dynamicNameTimeZone;

    public IndexAction(String index, String docType, @Nullable String executionTimeField,
                       @Nullable TimeValue timeout, @Nullable DateTimeZone dynamicNameTimeZone) {
        this.index = index;
        this.docType = docType;
        this.executionTimeField = executionTimeField;
        this.timeout = timeout;
        this.dynamicNameTimeZone = dynamicNameTimeZone;
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

    public DateTimeZone getDynamicNameTimeZone() {
        return dynamicNameTimeZone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexAction that = (IndexAction) o;

        if (!index.equals(that.index)) return false;
        if (!docType.equals(that.docType)) return false;
        if (executionTimeField != null ? !executionTimeField.equals(that.executionTimeField) : that.executionTimeField != null)
            return false;
        if (timeout != null ? !timeout.equals(that.timeout) : that.timeout != null) return false;
        return !(dynamicNameTimeZone != null ? !dynamicNameTimeZone.equals(that.dynamicNameTimeZone) : that.dynamicNameTimeZone != null);
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + docType.hashCode();
        result = 31 * result + (executionTimeField != null ? executionTimeField.hashCode() : 0);
        result = 31 * result + (timeout != null ? timeout.hashCode() : 0);
        result = 31 * result + (dynamicNameTimeZone != null ? dynamicNameTimeZone.hashCode() : 0);
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
        if (timeout != null) {
            builder.field(Field.TIMEOUT.getPreferredName(), timeout);
        }
        if (dynamicNameTimeZone != null) {
            builder.field(Field.DYNAMIC_NAME_TIMEZONE.getPreferredName(), dynamicNameTimeZone);
        }
        return builder.endObject();
    }

    public static IndexAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String index = null;
        String docType = null;
        String executionTimeField = null;
        TimeValue timeout = null;
        DateTimeZone dynamicNameTimeZone = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.INDEX)) {
                try {
                    index = parser.text();
                } catch (ElasticsearchParseException pe) {
                    throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. failed to parse index name value for " +
                            "field [{}]", pe, TYPE, watchId, actionId, currentFieldName);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.DOC_TYPE)) {
                    docType = parser.text();
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.EXECUTION_TIME_FIELD)) {
                    executionTimeField = parser.text();
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.TIMEOUT)) {
                    timeout = WatcherDateTimeUtils.parseTimeValue(parser, Field.TIMEOUT.toString());
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.DYNAMIC_NAME_TIMEZONE)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        dynamicNameTimeZone = DateTimeZone.forID(parser.text());
                    } else {
                        throw new ElasticsearchParseException("could not parse [{}] action for watch [{}]. failed to parse [{}]. must be " +
                                "a string value (e.g. 'UTC' or '+01:00').", TYPE, watchId, currentFieldName);
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. unexpected string field [{}]", TYPE,
                            watchId, actionId, currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. unexpected token [{}]", TYPE, watchId,
                        actionId, token);
            }
        }

        if (index == null) {
            throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. missing required [{}] field", TYPE, watchId,
                    actionId, Field.INDEX.getPreferredName());
        }

        if (docType == null) {
            throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. missing required [{}] field", TYPE, watchId,
                    actionId, Field.DOC_TYPE.getPreferredName());
        }

        return new IndexAction(index, docType, executionTimeField, timeout, dynamicNameTimeZone);
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
        TimeValue timeout;
        DateTimeZone dynamicNameTimeZone;

        private Builder(String index, String docType) {
            this.index = index;
            this.docType = docType;
        }

        public Builder setExecutionTimeField(String executionTimeField) {
            this.executionTimeField = executionTimeField;
            return this;
        }

        public Builder timeout(TimeValue writeTimeout) {
            this.timeout = writeTimeout;
            return this;
        }

        public Builder dynamicNameTimeZone(DateTimeZone dynamicNameTimeZone) {
            this.dynamicNameTimeZone = dynamicNameTimeZone;
            return this;
        }

        @Override
        public IndexAction build() {
            return new IndexAction(index, docType, executionTimeField, timeout, dynamicNameTimeZone);
        }
    }

    interface Field extends Action.Field {
        ParseField INDEX = new ParseField("index");
        ParseField DOC_TYPE = new ParseField("doc_type");
        ParseField EXECUTION_TIME_FIELD = new ParseField("execution_time_field");
        ParseField SOURCE = new ParseField("source");
        ParseField RESPONSE = new ParseField("response");
        ParseField REQUEST = new ParseField("request");
        ParseField TIMEOUT = new ParseField("timeout");
        ParseField DYNAMIC_NAME_TIMEZONE = new ParseField("dynamic_name_timezone");
    }
}
