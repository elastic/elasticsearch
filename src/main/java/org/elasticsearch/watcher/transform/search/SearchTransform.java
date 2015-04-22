/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.SearchRequestEquivalence;
import org.elasticsearch.watcher.support.SearchRequestParseException;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class SearchTransform implements Transform {

    public static final String TYPE = "search";

    protected final SearchRequest request;

    public SearchTransform(SearchRequest request) {
        this.request = request;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public SearchRequest getRequest() {
        return request;
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
        return WatcherUtils.writeSearchRequest(request, builder, params);
    }

    public static SearchTransform parse(String watchId, XContentParser parser) throws IOException {
        try {
            SearchRequest request = WatcherUtils.readSearchRequest(parser, ExecutableSearchTransform.DEFAULT_SEARCH_TYPE);
            return new SearchTransform(request);
        } catch (SearchRequestParseException srpe) {
            throw new SearchTransformException("could not parse [{}] transform for watch [{}]. failed parsing search request", srpe, TYPE, watchId);
        }
    }

    public static Builder builder(SearchRequest request) {
        return new Builder(request);
    }

    public static class Result extends Transform.Result {

        public Result(Payload payload) {
            super(TYPE, payload);
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        public static Result parse(String watchId, XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new SearchTransformException("could not parse [{}] transform result for watch [{}]. expected an object, but found [{}] instead", TYPE, watchId, token);
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.FIELD_NAME || !Field.PAYLOAD.match(parser.currentName())) {
                throw new SearchTransformException("could not parse [{}] transform result for watch [{}]. expected a [{}] field, but found [{}] instead", TYPE, watchId, Field.PAYLOAD.getPreferredName(), token);
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new SearchTransformException("could not parse [{}] transform result for watch [{}]. expected a [{}] field, but found [{}] instead", TYPE, watchId, Field.PAYLOAD.getPreferredName(), token);
            }
            return new SearchTransform.Result(new Payload.XContent(parser));
        }
    }

    public static class Builder implements Transform.Builder<SearchTransform> {

        private final SearchRequest request;

        public Builder(SearchRequest request) {
            this.request = request;
        }

        @Override
        public SearchTransform build() {
            return new SearchTransform(request);
        }
    }
}
