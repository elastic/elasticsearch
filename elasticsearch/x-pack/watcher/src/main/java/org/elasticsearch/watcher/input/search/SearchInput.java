/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

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
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.SearchRequestEquivalence;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 *
 */
public class SearchInput implements Input {

    public static final String TYPE = "search";

    private final SearchRequest searchRequest;
    private final @Nullable Set<String> extractKeys;
    private final @Nullable TimeValue timeout;
    private final @Nullable DateTimeZone dynamicNameTimeZone;

    public SearchInput(SearchRequest searchRequest, @Nullable Set<String> extractKeys,
                       @Nullable TimeValue timeout, @Nullable DateTimeZone dynamicNameTimeZone) {
        this.searchRequest = searchRequest;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchInput that = (SearchInput) o;

        if (!SearchRequestEquivalence.INSTANCE.equivalent(searchRequest, this.searchRequest)) return false;
        if (extractKeys != null ? !extractKeys.equals(that.extractKeys) : that.extractKeys != null) return false;
        if (timeout != null ? !timeout.equals(that.timeout) : that.timeout != null) return false;
        return !(dynamicNameTimeZone != null ? !dynamicNameTimeZone.equals(that.dynamicNameTimeZone) : that.dynamicNameTimeZone != null);
    }

    @Override
    public int hashCode() {
        int result = searchRequest.hashCode();
        result = 31 * result + (extractKeys != null ? extractKeys.hashCode() : 0);
        result = 31 * result + (timeout != null ? timeout.hashCode() : 0);
        result = 31 * result + (dynamicNameTimeZone != null ? dynamicNameTimeZone.hashCode() : 0);
        return result;
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public Set<String> getExtractKeys() {
        return extractKeys;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public DateTimeZone getDynamicNameTimeZone() {
        return dynamicNameTimeZone;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.REQUEST.getPreferredName());
        builder = WatcherUtils.writeSearchRequest(searchRequest, builder, params);
        if (extractKeys != null) {
            builder.field(Field.EXTRACT.getPreferredName(), extractKeys);
        }
        if (timeout != null) {
            builder.field(Field.TIMEOUT.getPreferredName(), timeout);
        }
        if (dynamicNameTimeZone != null) {
            builder.field(Field.DYNAMIC_NAME_TIMEZONE.getPreferredName(), dynamicNameTimeZone);
        }
        builder.endObject();
        return builder;
    }

    public static SearchInput parse(String watchId, XContentParser parser, QueryParseContext context,
                                    AggregatorParsers aggParsers, Suggesters suggesters)
            throws IOException {
        SearchRequest request = null;
        Set<String> extract = null;
        TimeValue timeout = null;
        DateTimeZone dynamicNameTimeZone = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.REQUEST)) {
                try {
                    request = WatcherUtils.readSearchRequest(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE, context,
                                                             aggParsers, suggesters);
                } catch (ElasticsearchParseException srpe) {
                    throw new ElasticsearchParseException("could not parse [{}] input for watch [{}]. failed to parse [{}]", srpe, TYPE,
                            watchId, currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.EXTRACT)) {
                    extract = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            extract.add(parser.text());
                        } else {
                            throw new ElasticsearchParseException("could not parse [{}] input for watch [{}]. expected a string value in " +
                                    "[{}] array, but found [{}] instead", TYPE, watchId, currentFieldName, token);
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] input for watch [{}]. unexpected array field [{}]", TYPE,
                            watchId, currentFieldName);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.TIMEOUT)) {
                timeout = WatcherDateTimeUtils.parseTimeValue(parser, Field.TIMEOUT.toString());
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.DYNAMIC_NAME_TIMEZONE)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    dynamicNameTimeZone = DateTimeZone.forID(parser.text());
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] input for watch [{}]. failed to parse [{}]. must be a " +
                            "string value (e.g. 'UTC' or '+01:00').", TYPE, watchId, currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("could not parse [{}] input for watch [{}]. unexpected token [{}]", TYPE, watchId,
                        token);
            }
        }

        if (request == null) {
            throw new ElasticsearchParseException("could not parse [{}] input for watch [{}]. missing required [{}] field", TYPE,
                    watchId, Field.REQUEST.getPreferredName());
        }
        return new SearchInput(request, extract, timeout, dynamicNameTimeZone);
    }

    public static Builder builder(SearchRequest request) {
        return new Builder(request);
    }

    public static class Result extends Input.Result {

        private final @Nullable SearchRequest request;

        public Result(SearchRequest request, Payload payload) {
            super(TYPE, payload);
            this.request = request;
        }

        public Result(@Nullable SearchRequest request, Exception e) {
            super(TYPE, e);
            this.request = request;
        }

        public SearchRequest executedRequest() {
            return request;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (request == null) {
                return builder;
            }
            builder.startObject(type);
            builder.field(Field.REQUEST.getPreferredName());
            WatcherUtils.writeSearchRequest(request, builder, params);
            return builder.endObject();
        }
    }

    public static class Builder implements Input.Builder<SearchInput> {

        private final SearchRequest request;
        private final Set<String> extractKeys = new HashSet<>();
        private TimeValue timeout;
        private DateTimeZone dynamicNameTimeZone;

        private Builder(SearchRequest request) {
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

        public Builder dynamicNameTimeZone(DateTimeZone dynamicNameTimeZone) {
            this.dynamicNameTimeZone = dynamicNameTimeZone;
            return this;
        }

        @Override
        public SearchInput build() {
            return new SearchInput(request, extractKeys.isEmpty() ? null : unmodifiableSet(extractKeys), timeout, dynamicNameTimeZone);
        }
    }

    public interface Field extends Input.Field {
        ParseField REQUEST = new ParseField("request");
        ParseField EXTRACT = new ParseField("extract");
        ParseField TIMEOUT = new ParseField("timeout");
        ParseField DYNAMIC_NAME_TIMEZONE = new ParseField("dynamic_name_timezone");
    }
}
