/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.WatcherDateTimeUtils.formatDate;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTime;

/**
 */
public final class WatcherUtils {

    static final ParseField INDICES_FIELD = new ParseField("indices");
    static final ParseField TYPES_FIELD = new ParseField("types");
    static final ParseField BODY_FIELD = new ParseField("body");
    static final ParseField SEARCH_TYPE_FIELD = new ParseField("search_type");
    static final ParseField INDICES_OPTIONS_FIELD = new ParseField("indices_options");
    static final ParseField EXPAND_WILDCARDS_FIELD = new ParseField("expand_wildcards");
    static final ParseField IGNORE_UNAVAILABLE_FIELD = new ParseField("ignore_unavailable");
    static final ParseField ALLOW_NO_INDICES_FIELD = new ParseField("allow_no_indices");
    static final ParseField TEMPLATE_FIELD = new ParseField("template");

    public final static IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.lenientExpandOpen();

    private WatcherUtils() {
    }

    public static Map<String, Object> responseToData(ToXContent response) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().value(response).endObject();
        return XContentHelper.convertToMap(builder.bytes(), false).v2();
    }

    public static SearchRequest createSearchRequestFromPrototype(SearchRequest requestPrototype, WatchExecutionContext ctx,
                                                                 Payload payload) throws IOException {
        SearchRequest request = new SearchRequest()
                .indicesOptions(requestPrototype.indicesOptions())
                .searchType(requestPrototype.searchType())
                .indices(requestPrototype.indices())
                .types(requestPrototype.types());

        // Due the inconsistency with templates in ES 1.x, we maintain our own template format.
        // This template format we use now, will become the template structure in ES 2.0
        Map<String, Object> watcherContextParams = Variables.createCtxModel(ctx, payload);
        if (requestPrototype.source() != null) {
            // Here we convert a watch search request body into an inline search template,
            // this way if any Watcher related context variables are used, they will get resolved,
            // by ES search template support
            XContentBuilder builder = jsonBuilder();
            requestPrototype.source().toXContent(builder, ToXContent.EMPTY_PARAMS);
            Template template = new Template(builder.string(), ScriptType.INLINE, null, builder.contentType(), watcherContextParams);
            request.template(template);
        } else if (requestPrototype.template() != null) {
            // Here we convert watcher template into a ES core templates. Due to the different format we use, we
            // convert to the template format used in ES core
            Template template = requestPrototype.template();
            if (template.getParams() != null) {
                watcherContextParams.putAll(template.getParams());
            }
            template = new Template(template.getScript(), template.getType(), template.getLang(), template.getContentType(),
                    watcherContextParams);
            request.template(template);
            // }
        }
        // falling back to an empty body
        return request;
    }


    /**
     * Reads a new search request instance for the specified parser.
     */
    public static SearchRequest readSearchRequest(XContentParser parser, SearchType searchType, QueryParseContext context,
                                                  AggregatorParsers aggParsers, Suggesters suggesters)
            throws IOException {
        IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
        SearchRequest searchRequest = new SearchRequest();

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
                    Template template = Template.parse(parser, ParseFieldMatcher.STRICT);
                    searchRequest.template(template);
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
        return searchRequest;
    }

    /**
     * Writes the searchRequest to the specified builder.
     */
    public static XContentBuilder writeSearchRequest(SearchRequest searchRequest, XContentBuilder builder,
                                                     ToXContent.Params params) throws IOException {
        if (searchRequest == null) {
            builder.nullValue();
            return builder;
        }

        builder.startObject();
        if (searchRequest.searchType() != null) {
            builder.field(SEARCH_TYPE_FIELD.getPreferredName(), searchRequest.searchType().toString().toLowerCase(Locale.ENGLISH));
        }
        if (searchRequest.indices() != null) {
            builder.array(INDICES_FIELD.getPreferredName(), searchRequest.indices());
        }
        if (searchRequest.types() != null) {
            builder.array(TYPES_FIELD.getPreferredName(), searchRequest.types());
        }
        if (searchRequest.source() != null) {
            builder.field(BODY_FIELD.getPreferredName(), searchRequest.source());
        }
        if (searchRequest.template() != null) {
            builder.field(TEMPLATE_FIELD.getPreferredName(), searchRequest.template());
        }

        if (searchRequest.indicesOptions() != DEFAULT_INDICES_OPTIONS) {
            IndicesOptions options = searchRequest.indicesOptions();
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
        return builder.endObject();
    }

    public static Map<String, Object> flattenModel(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        flattenModel("", map, result);
        return result;
    }

    private static void flattenModel(String key, Object value, Map<String, Object> result) {
        if (value == null) {
            result.put(key, null);
            return;
        }
        if (value instanceof Map) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                if ("".equals(key)) {
                    flattenModel(entry.getKey(), entry.getValue(), result);
                } else {
                    flattenModel(key + "." + entry.getKey(), entry.getValue(), result);
                }
            }
            return;
        }
        if (value instanceof Iterable) {
            int i = 0;
            for (Object item : (Iterable) value) {
                flattenModel(key + "." + i++, item, result);
            }
            return;
        }
        if (value.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(value); i++) {
                flattenModel(key + "." + i, Array.get(value, i), result);
            }
            return;
        }
        if (value instanceof DateTime) {
            result.put(key, formatDate((DateTime) value));
            return;
        }
        if (value instanceof TimeValue) {
            result.put(key, String.valueOf(((TimeValue) value).getMillis()));
            return;
        }
        result.put(key, String.valueOf(value));
    }
}
