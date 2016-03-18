/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.watcher.support.SearchRequestEquivalence;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 *
 */
public class SearchTransform implements Transform {

    public static final String TYPE = "search";

    private final SearchRequest request;
    private final @Nullable TimeValue timeout;
    private final @Nullable DateTimeZone dynamicNameTimeZone;

    public SearchTransform(SearchRequest request, @Nullable TimeValue timeout, @Nullable DateTimeZone dynamicNameTimeZone) {
        this.request = request;
        this.timeout = timeout;
        this.dynamicNameTimeZone = dynamicNameTimeZone;
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

    public DateTimeZone getDynamicNameTimeZone() {
        return dynamicNameTimeZone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchTransform that = (SearchTransform) o;

        if (!SearchRequestEquivalence.INSTANCE.equivalent(request, this.request)) return false;
        if (timeout != null ? !timeout.equals(that.timeout) : that.timeout != null) return false;
        return !(dynamicNameTimeZone != null ? !dynamicNameTimeZone.equals(that.dynamicNameTimeZone) : that.dynamicNameTimeZone != null);
    }

    @Override
    public int hashCode() {
        int result = request.hashCode();
        result = 31 * result + (timeout != null ? timeout.hashCode() : 0);
        result = 31 * result + (dynamicNameTimeZone != null ? dynamicNameTimeZone.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.REQUEST.getPreferredName());
        builder = WatcherUtils.writeSearchRequest(request, builder, params);
        if (timeout != null) {
            builder.field(Field.TIMEOUT.getPreferredName(), timeout);
        }
        if (dynamicNameTimeZone != null) {
            builder.field(Field.DYNAMIC_NAME_TIMEZONE.getPreferredName(), dynamicNameTimeZone);
        }
        builder.endObject();
        return builder;
    }

    public static SearchTransform parse(String watchId, XContentParser parser, QueryParseContext context,
                                        AggregatorParsers aggParsers, Suggesters suggesters)
            throws IOException {
        SearchRequest request = null;
        TimeValue timeout = null;
        DateTimeZone dynamicNameTimeZone = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.REQUEST)) {
                try {
                    request = WatcherUtils.readSearchRequest(parser, ExecutableSearchTransform.DEFAULT_SEARCH_TYPE, context,
                                                             aggParsers, suggesters);
                } catch (ElasticsearchParseException srpe) {
                    throw new ElasticsearchParseException("could not parse [{}] transform for watch [{}]. failed to parse [{}]", srpe,
                            TYPE, watchId, currentFieldName);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.TIMEOUT)) {
                timeout = WatcherDateTimeUtils.parseTimeValue(parser, Field.TIMEOUT.toString());
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.DYNAMIC_NAME_TIMEZONE)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    dynamicNameTimeZone = DateTimeZone.forID(parser.text());
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] transform for watch [{}]. failed to parse [{}]. must be a" +
                            " string value (e.g. 'UTC' or '+01:00').", TYPE, watchId, currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("could not parse [{}] transform for watch [{}]. unexpected field [{}]", TYPE,
                        watchId, currentFieldName);
            }
        }

        if (request == null) {
            throw new ElasticsearchParseException("could not parse [{}] transform for watch [{}]. missing required [{}] field", TYPE,
                    watchId, Field.REQUEST.getPreferredName());
        }
        return new SearchTransform(request, timeout, dynamicNameTimeZone);
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
        private DateTimeZone dynamicNameTimeZone;

        public Builder(SearchRequest request) {
            this.request = request;
        }

        public Builder timeout(TimeValue readTimeout) {
            this.timeout = readTimeout;
            return this;
        }

        public Builder dynamicNameTimeZone(DateTimeZone dynamicNameTimeZone) {
            this.dynamicNameTimeZone = dynamicNameTimeZone;
            return this;
        }

        @Override
        public SearchTransform build() {
            return new SearchTransform(request, timeout, dynamicNameTimeZone);
        }
    }

    public interface Field extends Transform.Field {
        ParseField REQUEST = new ParseField("request");
        ParseField TIMEOUT = new ParseField("timeout");
        ParseField DYNAMIC_NAME_TIMEZONE = new ParseField("dynamic_name_timezone");
    }
}
