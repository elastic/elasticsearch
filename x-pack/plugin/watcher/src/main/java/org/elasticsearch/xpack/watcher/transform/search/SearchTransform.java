/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transform.search;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.core.TimeValue.timeValueMillis;

public class SearchTransform implements Transform {

    public static final String TYPE = "search";

    private final WatcherSearchTemplateRequest request;
    @Nullable
    private final TimeValue timeout;
    @Nullable
    private final ZoneId dynamicNameTimeZone;

    public SearchTransform(WatcherSearchTemplateRequest request, @Nullable TimeValue timeout, @Nullable ZoneId dynamicNameTimeZone) {
        this.request = request;
        this.timeout = timeout;
        this.dynamicNameTimeZone = dynamicNameTimeZone;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public WatcherSearchTemplateRequest getRequest() {
        return request;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public ZoneId getDynamicNameTimeZone() {
        return dynamicNameTimeZone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchTransform that = (SearchTransform) o;
        return Objects.equals(request, that.request)
            && Objects.equals(timeout, that.timeout)
            && Objects.equals(dynamicNameTimeZone, that.dynamicNameTimeZone);
    }

    @Override
    public int hashCode() {
        int result = request != null ? request.hashCode() : 0;
        result = 31 * result + (timeout != null ? timeout.hashCode() : 0);
        result = 31 * result + (dynamicNameTimeZone != null ? dynamicNameTimeZone.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (request != null) {
            builder.field(Field.REQUEST.getPreferredName(), request);
        }
        if (timeout != null) {
            builder.humanReadableField(Field.TIMEOUT.getPreferredName(), Field.TIMEOUT_HUMAN.getPreferredName(), timeout);
        }
        if (dynamicNameTimeZone != null) {
            builder.field(Field.DYNAMIC_NAME_TIMEZONE.getPreferredName(), dynamicNameTimeZone.toString());
        }
        builder.endObject();
        return builder;
    }

    public static SearchTransform parse(String watchId, XContentParser parser) throws IOException {
        WatcherSearchTemplateRequest request = null;
        TimeValue timeout = null;
        ZoneId dynamicNameTimeZone = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.REQUEST.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    request = WatcherSearchTemplateRequest.fromXContent(parser, ExecutableSearchTransform.DEFAULT_SEARCH_TYPE);
                } catch (ElasticsearchParseException srpe) {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] transform for watch [{}]. failed to parse [{}]",
                        srpe,
                        TYPE,
                        watchId,
                        currentFieldName
                    );
                }
            } else if (Field.TIMEOUT.match(currentFieldName, parser.getDeprecationHandler())) {
                timeout = timeValueMillis(parser.longValue());
            } else if (Field.TIMEOUT_HUMAN.match(currentFieldName, parser.getDeprecationHandler())) {
                // Parser for human specified timeouts and 2.x compatibility
                timeout = WatcherDateTimeUtils.parseTimeValue(parser, Field.TIMEOUT_HUMAN.toString());
            } else if (Field.DYNAMIC_NAME_TIMEZONE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    dynamicNameTimeZone = DateUtils.of(parser.text());
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] transform for watch [{}]. failed to parse [{}]. must be a"
                            + " string value (e.g. 'UTC' or '+01:00').",
                        TYPE,
                        watchId,
                        currentFieldName
                    );
                }
            } else {
                throw new ElasticsearchParseException(
                    "could not parse [{}] transform for watch [{}]. unexpected field [{}]",
                    TYPE,
                    watchId,
                    currentFieldName
                );
            }
        }

        if (request == null) {
            throw new ElasticsearchParseException(
                "could not parse [{}] transform for watch [{}]. missing required [{}] field",
                TYPE,
                watchId,
                Field.REQUEST.getPreferredName()
            );
        }
        return new SearchTransform(request, timeout, dynamicNameTimeZone);
    }

    public static Builder builder(WatcherSearchTemplateRequest request) {
        return new Builder(request);
    }

    public static class Result extends Transform.Result {

        @Nullable
        private final WatcherSearchTemplateRequest request;

        public Result(WatcherSearchTemplateRequest request, Payload payload) {
            super(TYPE, payload);
            this.request = request;
        }

        public Result(WatcherSearchTemplateRequest request, Exception e) {
            super(TYPE, e);
            this.request = request;
        }

        public WatcherSearchTemplateRequest executedRequest() {
            return request;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (request != null) {
                builder.startObject(type);
                builder.field(Field.REQUEST.getPreferredName(), request);
                builder.endObject();
            }
            return builder;
        }
    }

    public static class Builder implements Transform.Builder<SearchTransform> {

        private final WatcherSearchTemplateRequest request;
        private TimeValue timeout;
        private ZoneId dynamicNameTimeZone;

        public Builder(WatcherSearchTemplateRequest request) {
            this.request = request;
        }

        public Builder timeout(TimeValue readTimeout) {
            this.timeout = readTimeout;
            return this;
        }

        public Builder dynamicNameTimeZone(ZoneId dynamicNameTimeZone) {
            this.dynamicNameTimeZone = dynamicNameTimeZone;
            return this;
        }

        @Override
        public SearchTransform build() {
            return new SearchTransform(request, timeout, dynamicNameTimeZone);
        }
    }

    public interface Field {
        ParseField REQUEST = new ParseField("request");
        ParseField TIMEOUT = new ParseField("timeout_in_millis");
        ParseField TIMEOUT_HUMAN = new ParseField("timeout");
        ParseField DYNAMIC_NAME_TIMEZONE = new ParseField("dynamic_name_timezone");
    }
}
