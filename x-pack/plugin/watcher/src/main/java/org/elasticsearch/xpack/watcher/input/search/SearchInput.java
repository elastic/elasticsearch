/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.search;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.core.TimeValue.timeValueMillis;

public class SearchInput implements Input {

    public static final String TYPE = "search";

    private final WatcherSearchTemplateRequest request;
    @Nullable
    private final Set<String> extractKeys;
    @Nullable
    private final TimeValue timeout;
    @Nullable
    private final ZoneId dynamicNameTimeZone;

    public SearchInput(
        WatcherSearchTemplateRequest request,
        @Nullable Set<String> extractKeys,
        @Nullable TimeValue timeout,
        @Nullable ZoneId dynamicNameTimeZone
    ) {
        this.request = request;
        this.extractKeys = extractKeys;
        this.timeout = timeout;
        this.dynamicNameTimeZone = dynamicNameTimeZone;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchInput that = (SearchInput) o;
        return Objects.equals(request, that.request)
            && Objects.equals(extractKeys, that.extractKeys)
            && Objects.equals(timeout, that.timeout)
            && Objects.equals(dynamicNameTimeZone, that.dynamicNameTimeZone);
    }

    @Override
    public int hashCode() {
        int result = request != null ? request.hashCode() : 0;
        result = 31 * result + (extractKeys != null ? extractKeys.hashCode() : 0);
        result = 31 * result + (timeout != null ? timeout.hashCode() : 0);
        result = 31 * result + (dynamicNameTimeZone != null ? dynamicNameTimeZone.hashCode() : 0);
        return result;
    }

    public WatcherSearchTemplateRequest getRequest() {
        return request;
    }

    public Set<String> getExtractKeys() {
        return extractKeys;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public ZoneId getDynamicNameTimeZone() {
        return dynamicNameTimeZone;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (request != null) {
            builder.field(Field.REQUEST.getPreferredName(), request);
        }
        if (extractKeys != null) {
            builder.field(Field.EXTRACT.getPreferredName(), extractKeys);
        }
        if (timeout != null) {
            builder.humanReadableField(Field.TIMEOUT.getPreferredName(), Field.TIMEOUT_HUMAN.getPreferredName(), timeout);
        }
        if (dynamicNameTimeZone != null) {
            builder.field(Field.DYNAMIC_NAME_TIMEZONE.getPreferredName(), dynamicNameTimeZone);
        }
        builder.endObject();
        return builder;
    }

    public static SearchInput parse(String watchId, XContentParser parser) throws IOException {
        WatcherSearchTemplateRequest request = null;
        Set<String> extract = null;
        TimeValue timeout = null;
        ZoneId dynamicNameTimeZone = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.REQUEST.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    request = WatcherSearchTemplateRequest.fromXContent(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE);
                } catch (ElasticsearchParseException srpe) {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] input for watch [{}]. failed to parse [{}]",
                        srpe,
                        TYPE,
                        watchId,
                        currentFieldName
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Field.EXTRACT.match(currentFieldName, parser.getDeprecationHandler())) {
                    extract = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            extract.add(parser.text());
                        } else {
                            throw new ElasticsearchParseException(
                                "could not parse [{}] input for watch [{}]. expected a string value in "
                                    + "[{}] array, but found [{}] instead",
                                TYPE,
                                watchId,
                                currentFieldName,
                                token
                            );
                        }
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] input for watch [{}]. unexpected array field [{}]",
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
                        "could not parse [{}] input for watch [{}]. failed to parse [{}]. must be a "
                            + "string value (e.g. 'UTC' or '+01:00').",
                        TYPE,
                        watchId,
                        currentFieldName
                    );
                }
            } else {
                throw new ElasticsearchParseException(
                    "could not parse [{}] input for watch [{}]. unexpected token [{}]",
                    TYPE,
                    watchId,
                    token
                );
            }
        }

        if (request == null) {
            throw new ElasticsearchParseException(
                "could not parse [{}] input for watch [{}]. missing required [{}] field",
                TYPE,
                watchId,
                Field.REQUEST.getPreferredName()
            );
        }
        return new SearchInput(request, extract, timeout, dynamicNameTimeZone);
    }

    public static Builder builder(WatcherSearchTemplateRequest request) {
        return new Builder(request);
    }

    public static class Result extends Input.Result {

        @Nullable
        private final WatcherSearchTemplateRequest request;

        public Result(WatcherSearchTemplateRequest request, Payload payload) {
            super(TYPE, payload);
            this.request = request;
        }

        public Result(@Nullable WatcherSearchTemplateRequest request, Exception e) {
            super(TYPE, e);
            this.request = request;
        }

        public WatcherSearchTemplateRequest executedRequest() {
            return request;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (request == null) {
                return builder;
            }
            builder.startObject(type);
            builder.field(Field.REQUEST.getPreferredName(), request);
            return builder.endObject();
        }
    }

    public static class Builder implements Input.Builder<SearchInput> {

        private final WatcherSearchTemplateRequest request;
        private final Set<String> extractKeys = new HashSet<>();
        private TimeValue timeout;
        private ZoneId dynamicNameTimeZone;

        private Builder(WatcherSearchTemplateRequest request) {
            this.request = request;
        }

        public Builder extractKeys(Collection<String> keys) {
            extractKeys.addAll(keys);
            return this;
        }

        public Builder extractKeys(String... keys) {
            Collections.addAll(extractKeys, keys);
            return this;
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
        public SearchInput build() {
            return new SearchInput(request, extractKeys.isEmpty() ? null : unmodifiableSet(extractKeys), timeout, dynamicNameTimeZone);
        }
    }

    public interface Field {
        ParseField REQUEST = new ParseField("request");
        ParseField EXTRACT = new ParseField("extract");
        ParseField TIMEOUT = new ParseField("timeout_in_millis");
        ParseField TIMEOUT_HUMAN = new ParseField("timeout");
        ParseField DYNAMIC_NAME_TIMEZONE = new ParseField("dynamic_name_timezone");
    }
}
