/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.SearchRequestEquivalence;
import org.elasticsearch.watcher.support.SearchRequestParseException;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class SearchInput implements Input {

    public static final String TYPE = "search";

    private final SearchRequest searchRequest;
    private final @Nullable Set<String> extractKeys;

    public SearchInput(SearchRequest searchRequest, @Nullable Set<String> extractKeys) {
        this.searchRequest = searchRequest;
        this.extractKeys = extractKeys;
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
        return !(extractKeys != null ? !extractKeys.equals(that.extractKeys) : that.extractKeys != null);
    }

    @Override
    public int hashCode() {
        int result = searchRequest.hashCode();
        result = 31 * result + (extractKeys != null ? extractKeys.hashCode() : 0);
        return result;
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public Set<String> getExtractKeys() {
        return extractKeys;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.REQUEST.getPreferredName());
        builder = WatcherUtils.writeSearchRequest(searchRequest, builder, params);
        if (extractKeys != null) {
            builder.field(Field.EXTRACT.getPreferredName(), extractKeys);
        }
        builder.endObject();
        return builder;
    }

    public static SearchInput parse(String watchId, XContentParser parser) throws IOException {
        SearchRequest request = null;
        Set<String> extract = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.REQUEST.match(currentFieldName)) {
                try {
                    request = WatcherUtils.readSearchRequest(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE);
                } catch (SearchRequestParseException srpe) {
                    throw new SearchInputException("could not parse [{}] input for watch [{}]. failed to parse [{}]", srpe, TYPE, watchId, currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Field.EXTRACT.match(currentFieldName)) {
                    extract = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            extract.add(parser.text());
                        } else {
                            throw new SearchInputException("could not parse [{}] input for watch [{}]. expected a string value in [{}] array, but found [{}] instead", TYPE, watchId, currentFieldName, token);
                        }
                    }
                } else {
                    throw new SearchInputException("could not parse [{}] input for watch [{}]. unexpected array field [{}]", TYPE, watchId, currentFieldName);
                }
            } else {
                throw new SearchInputException("could not parse [{}] input for watch [{}]. unexpected token [{}]", TYPE, watchId, token);
            }
        }

        if (request == null) {
            throw new SearchInputException("could not parse [{}] input for watch [{}]. missing required [{}] field", TYPE, watchId, Field.REQUEST.getPreferredName());
        }

        return new SearchInput(request, extract);
    }

    public static Builder builder(SearchRequest request) {
        return new Builder(request);
    }

    public static class Result extends Input.Result {

        private final SearchRequest executedRequest;

        public Result(SearchRequest executedRequest, Payload payload) {
            super(TYPE, payload);
            this.executedRequest = executedRequest;
        }

        public SearchRequest executedRequest() {
            return executedRequest;
        }

        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(Field.EXECUTED_REQUEST.getPreferredName());
            WatcherUtils.writeSearchRequest(executedRequest, builder, params);
            return builder;
        }

        public static Result parse(String watchId, XContentParser parser) throws IOException {
            SearchRequest executedRequest = null;
            Payload payload = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.EXECUTED_REQUEST.match(currentFieldName)) {
                    try {
                        executedRequest = WatcherUtils.readSearchRequest(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE);
                    } catch (SearchRequestParseException srpe) {
                        throw new SearchInputException("could not parse [{}] input result for watch [{}]. failed to parse [{}]", srpe, TYPE, watchId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if (Field.PAYLOAD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else {
                        throw new SearchInputException("could not parse [{}] input result for watch [{}]. unexpected field [{}]", TYPE, watchId, currentFieldName);
                    }
                }
            }

            if (executedRequest == null) {
                throw new SearchInputException("could not parse [{}] input result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.EXECUTED_REQUEST.getPreferredName());
            }

            if (payload == null) {
                throw new SearchInputException("could not parse [{}] input result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.PAYLOAD.getPreferredName());
            }

            return new Result(executedRequest, payload);
        }
    }

    public static class Builder implements Input.Builder<SearchInput> {

        private final SearchRequest request;
        private final ImmutableSet.Builder<String> extractKeys = ImmutableSet.builder();

        private Builder(SearchRequest request) {
            this.request = request;
        }

        public Builder extractKeys(Collection<String> keys) {
            extractKeys.addAll(keys);
            return this;
        }

        public Builder extractKeys(String... keys) {
            extractKeys.add(keys);
            return this;
        }

        @Override
        public SearchInput build() {
            Set<String> keys = extractKeys.build();
            return new SearchInput(request, keys.isEmpty() ? null : keys);
        }
    }

    public interface Field extends Input.Field {
        ParseField REQUEST = new ParseField("request");
        ParseField EXECUTED_REQUEST = new ParseField("executed_request");
        ParseField EXTRACT = new ParseField("extract");
    }
}
