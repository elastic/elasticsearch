/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.SearchRequestEquivalence;
import org.elasticsearch.watcher.support.SearchRequestParseException;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class SearchTransform implements Transform {

    public static final String TYPE = "search";

    private final SearchRequest request;
    private final @Nullable TimeValue timeout;

    public SearchTransform(SearchRequest request, @Nullable TimeValue timeout) {
        this.request = request;
        this.timeout = timeout;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public SearchRequest getRequest() {
        return request;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchTransform transform = (SearchTransform) o;

        return SearchRequestEquivalence.INSTANCE.equivalent(request, transform.request);
    }

    @Override
    public int hashCode() {
        return request.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.REQUEST.getPreferredName());
        builder = WatcherUtils.writeSearchRequest(request, builder, params);
        if (timeout != null) {
            builder.field(Field.TIMEOUT.getPreferredName(), timeout);
        }
        builder.endObject();
        return builder;
    }

    public static SearchTransform parse(String watchId, XContentParser parser, TimeValue defaultTimeout) throws IOException {
        SearchRequest request = null;
        TimeValue timeout = defaultTimeout;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.REQUEST.match(currentFieldName)) {
                try {
                    request = WatcherUtils.readSearchRequest(parser, ExecutableSearchTransform.DEFAULT_SEARCH_TYPE);
                } catch (SearchRequestParseException srpe) {
                    throw new SearchTransformException("could not parse [{}] transform for watch [{}]. failed to parse [{}]", srpe, TYPE, watchId, currentFieldName);
                }
            } else if (Field.TIMEOUT.match(currentFieldName)) {
                timeout = WatcherDateTimeUtils.parseTimeValue(parser, Field.TIMEOUT.toString());
            } else {
                throw new SearchTransformException("could not parse [{}] transform for watch [{}]. unexpected field [{}]", TYPE, watchId, currentFieldName);
            }
        }

        if (request == null) {
            throw new SearchTransformException("could not parse [{}] transform for watch [{}]. missing required [{}] field", TYPE, watchId, Field.REQUEST.getPreferredName());
        }
        return new SearchTransform(request, timeout);
    }

    public static Builder builder(SearchRequest request) {
        return new Builder(request);
    }

    public static class Result extends Transform.Result {

        private final @Nullable SearchRequest request;

        public Result(SearchRequest request, Payload payload) {
            super(TYPE, payload);
            this.request = request;
        }

        public Result(SearchRequest request, Exception e) {
            super(TYPE, e);
            this.request = request;
        }

        public SearchRequest executedRequest() {
            return request;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (request != null) {
                builder.startObject(type);
                builder.field(Field.REQUEST.getPreferredName());
                WatcherUtils.writeSearchRequest(request, builder, params);
                builder.endObject();
            }
            return builder;
        }
    }

    public static class Builder implements Transform.Builder<SearchTransform> {

        private final SearchRequest request;
        private TimeValue timeout;

        public Builder(SearchRequest request) {
            this.request = request;
        }

        public Builder timeout(TimeValue readTimeout) {
            this.timeout = readTimeout;
            return this;
        }

        @Override
        public SearchTransform build() {
            return new SearchTransform(request, timeout);
        }
    }

    public interface Field extends Transform.Field {
        ParseField REQUEST = new ParseField("request");
        ParseField TIMEOUT = new ParseField("timeout");
    }
}
