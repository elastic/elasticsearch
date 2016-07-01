/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support.search;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.xpack.watcher.support.Script;
import org.elasticsearch.xpack.watcher.support.SearchRequestEquivalence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * A {@link WatcherSearchTemplateRequest} contains the search request and the eventual template that will
 * be rendered as a script by {@link WatcherSearchTemplateService} before being executed.
 */
public class WatcherSearchTemplateRequest implements ToXContent {

    private final SearchRequest request;
    @Nullable private final Script template;

    public WatcherSearchTemplateRequest(SearchRequest searchRequest, @Nullable Script template) {
        this.request = Objects.requireNonNull(searchRequest);
        this.template = template;
    }

    public WatcherSearchTemplateRequest(SearchRequest request) {
        this(request, null);
    }

    public SearchRequest getRequest() {
        return request;
    }

    public Script getTemplate() {
        return template;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (request != null) {
            if (request.searchType() != null) {
                builder.field(SEARCH_TYPE_FIELD.getPreferredName(), request.searchType().toString().toLowerCase(Locale.ENGLISH));
            }
            if (request.indices() != null) {
                builder.array(INDICES_FIELD.getPreferredName(), request.indices());
            }
            if (request.types() != null) {
                builder.array(TYPES_FIELD.getPreferredName(), request.types());
            }
            if (request.source() != null) {
                builder.field(BODY_FIELD.getPreferredName(), request.source());
            }
            if (request.indicesOptions() != DEFAULT_INDICES_OPTIONS) {
                IndicesOptions options = request.indicesOptions();
                builder.startObject(INDICES_OPTIONS_FIELD.getPreferredName());
                String value;
                if (options.expandWildcardsClosed() && options.expandWildcardsOpen()) {
                    value = "all";
                } else if (options.expandWildcardsOpen()) {
                    value = "open";
                } else if (options.expandWildcardsClosed()) {
                    value = "closed";
                } else {
                    value = "none";
                }
                builder.field(EXPAND_WILDCARDS_FIELD.getPreferredName(), value);
                builder.field(IGNORE_UNAVAILABLE_FIELD.getPreferredName(), options.ignoreUnavailable());
                builder.field(ALLOW_NO_INDICES_FIELD.getPreferredName(), options.allowNoIndices());
                builder.endObject();
            }
        }
        if (template != null) {
            builder.field(TEMPLATE_FIELD.getPreferredName(), template);
        }
        return builder.endObject();
    }


    /**
     * Reads a new watcher search request instance for the specified parser.
     */
    public static WatcherSearchTemplateRequest fromXContent(XContentParser parser, SearchType searchType, QueryParseContext context,
                                                            AggregatorParsers aggParsers, Suggesters suggesters)
            throws IOException {
        IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
        SearchRequest searchRequest = new SearchRequest();
        Script template = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                if (ParseFieldMatcher.STRICT.match(currentFieldName, BODY_FIELD)) {
                    searchRequest.source(SearchSourceBuilder.fromXContent(context, aggParsers, suggesters));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (ParseFieldMatcher.STRICT.match(currentFieldName, INDICES_FIELD)) {
                    List<String> indices = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            indices.add(parser.textOrNull());
                        } else {
                            throw new ElasticsearchParseException("could not read search request. expected string values in [" +
                                    currentFieldName + "] field, but instead found [" + token + "]");
                        }
                    }
                    searchRequest.indices(indices.toArray(new String[indices.size()]));
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, TYPES_FIELD)) {
                    List<String> types = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            types.add(parser.textOrNull());
                        } else {
                            throw new ElasticsearchParseException("could not read search request. expected string values in [" +
                                    currentFieldName + "] field, but instead found [" + token + "]");
                        }
                    }
                    searchRequest.types(types.toArray(new String[types.size()]));
                } else {
                    throw new ElasticsearchParseException("could not read search request. unexpected array field [" +
                            currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (ParseFieldMatcher.STRICT.match(currentFieldName, INDICES_OPTIONS_FIELD)) {
                    boolean expandOpen = DEFAULT_INDICES_OPTIONS.expandWildcardsOpen();
                    boolean expandClosed = DEFAULT_INDICES_OPTIONS.expandWildcardsClosed();
                    boolean allowNoIndices = DEFAULT_INDICES_OPTIONS.allowNoIndices();
                    boolean ignoreUnavailable = DEFAULT_INDICES_OPTIONS.ignoreUnavailable();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (ParseFieldMatcher.STRICT.match(currentFieldName, EXPAND_WILDCARDS_FIELD)) {
                                switch (parser.text()) {
                                    case "all":
                                        expandOpen = true;
                                        expandClosed = true;
                                        break;
                                    case "open":
                                        expandOpen = true;
                                        expandClosed = false;
                                        break;
                                    case "closed":
                                        expandOpen = false;
                                        expandClosed = true;
                                        break;
                                    case "none":
                                        expandOpen = false;
                                        expandClosed = false;
                                        break;
                                    default:
                                        throw new ElasticsearchParseException("could not read search request. unknown value [" +
                                                parser.text() + "] for [" + currentFieldName + "] field ");
                                }
                            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, IGNORE_UNAVAILABLE_FIELD)) {
                                ignoreUnavailable = parser.booleanValue();
                            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, ALLOW_NO_INDICES_FIELD)) {
                                allowNoIndices = parser.booleanValue();
                            } else {
                                throw new ElasticsearchParseException("could not read search request. unexpected index option [" +
                                        currentFieldName + "]");
                            }
                        } else {
                            throw new ElasticsearchParseException("could not read search request. unexpected object field [" +
                                    currentFieldName + "]");
                        }
                    }
                    indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandOpen, expandClosed,
                            DEFAULT_INDICES_OPTIONS);
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, TEMPLATE_FIELD)) {
                    template = Script.parse(parser);
                } else {
                    throw new ElasticsearchParseException("could not read search request. unexpected object field [" +
                            currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (ParseFieldMatcher.STRICT.match(currentFieldName, INDICES_FIELD)) {
                    String indicesStr = parser.text();
                    searchRequest.indices(Strings.delimitedListToStringArray(indicesStr, ",", " \t"));
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, TYPES_FIELD)) {
                    String typesStr = parser.text();
                    searchRequest.types(Strings.delimitedListToStringArray(typesStr, ",", " \t"));
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, SEARCH_TYPE_FIELD)) {
                    searchType = SearchType.fromString(parser.text().toLowerCase(Locale.ROOT), ParseFieldMatcher.EMPTY);
                } else {
                    throw new ElasticsearchParseException("could not read search request. unexpected string field [" +
                            currentFieldName + "]");
                }
            } else {
                throw new ElasticsearchParseException("could not read search request. unexpected token [" + token + "]");
            }
        }

        if (searchRequest.indices() == null) {
            searchRequest.indices(Strings.EMPTY_ARRAY);
        }
        searchRequest.searchType(searchType);
        searchRequest.indicesOptions(indicesOptions);
        return new WatcherSearchTemplateRequest(searchRequest, template);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatcherSearchTemplateRequest that = (WatcherSearchTemplateRequest) o;

        if (!SearchRequestEquivalence.INSTANCE.equivalent(request, that.request)) return false;
        return template != null ? template.equals(that.template) : that.template == null;

    }

    @Override
    public int hashCode() {
        int result = request != null ? request.hashCode() : 0;
        result = 31 * result + (template != null ? template.hashCode() : 0);
        return result;
    }

    static final ParseField INDICES_FIELD = new ParseField("indices");
    static final ParseField TYPES_FIELD = new ParseField("types");
    static final ParseField BODY_FIELD = new ParseField("body");
    static final ParseField SEARCH_TYPE_FIELD = new ParseField("search_type");
    static final ParseField INDICES_OPTIONS_FIELD = new ParseField("indices_options");
    static final ParseField EXPAND_WILDCARDS_FIELD = new ParseField("expand_wildcards");
    static final ParseField IGNORE_UNAVAILABLE_FIELD = new ParseField("ignore_unavailable");
    static final ParseField ALLOW_NO_INDICES_FIELD = new ParseField("allow_no_indices");
    static final ParseField TEMPLATE_FIELD = new ParseField("template");

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.lenientExpandOpen();
}
