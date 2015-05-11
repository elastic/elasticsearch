/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParseField;
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

        private final SearchRequest request;

        public Result(SearchRequest request, Payload payload) {
            super(TYPE, payload);
            this.request = request;
        }

        public SearchRequest executedRequest() {
            return request;
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(Field.REQUEST.getPreferredName());
            WatcherUtils.writeSearchRequest(request, builder, params);
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
                } else if (Field.REQUEST.match(currentFieldName)) {
                    try {
                        executedRequest = WatcherUtils.readSearchRequest(parser, ExecutableSearchTransform.DEFAULT_SEARCH_TYPE);
                    } catch (SearchRequestParseException srpe) {
                        throw new SearchTransformException("could not parse [{}] transform result for watch [{}]. failed to parse [{}]", srpe, TYPE, watchId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if (Field.PAYLOAD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else {
                        throw new SearchTransformException("could not parse [{}] transform result for watch [{}]. unexpected field [{}]", TYPE, watchId, currentFieldName);
                    }
                }
            }

            if (payload == null) {
                throw new SearchTransformException("could not parse [{}] transform result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.PAYLOAD.getPreferredName());
            }

            if (executedRequest == null) {
                throw new SearchTransformException("could not parse [{}] transform result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.REQUEST.getPreferredName());
            }

            return new SearchTransform.Result(executedRequest, payload);
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

    public interface Field extends Transform.Field {
        ParseField REQUEST = new ParseField("request");
    }
}
