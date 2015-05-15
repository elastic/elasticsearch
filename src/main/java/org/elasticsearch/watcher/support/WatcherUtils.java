/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.WatcherDateTimeUtils.formatDate;

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

    public static Map<String, Object> responseToData(ToXContent response) {
        try {
            XContentBuilder builder = jsonBuilder().startObject().value(response).endObject();
            return XContentHelper.convertToMap(builder.bytes(), false).v2();
        } catch (IOException ioe) {
            throw new WatcherException("failed to convert search response to script parameters", ioe);
        }
    }

    public static SearchRequest createSearchRequestFromPrototype(SearchRequest requestPrototype, @Nullable DynamicIndexName[] dynamicIndexNames, WatchExecutionContext ctx, Payload payload) throws IOException {

        String[] indices = dynamicIndexNames == null ?
                requestPrototype.indices() :
                DynamicIndexName.names(dynamicIndexNames, ctx.executionTime());

        SearchRequest request = new SearchRequest(requestPrototype)
                .indicesOptions(requestPrototype.indicesOptions())
                .searchType(requestPrototype.searchType())
                .indices(indices)
                .types(requestPrototype.types());

        // TODO: Revise this search template conversion code once search templates in core have been refactored once ES 2.0 is released.
        // Due the inconsistency with templates in ES 1.x, we maintain our own template format.
        // This template format we use now, will become the template structure in ES 2.0
        Map<String, Object> watcherContextParams = Variables.createCtxModel(ctx, payload);
        if (Strings.hasLength(requestPrototype.source())) {
            // Here we convert a watch search request body into an inline search template,
            // this way if any Watcher related context variables are used, they will get resolved,
            // by ES search template support
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            XContentHelper.writeRawField("template", requestPrototype.source(), builder, ToXContent.EMPTY_PARAMS);
            builder.field("params", watcherContextParams);
            builder.endObject();
            // Unfortunately because of SearchRequest#templateSource(BytesReference, boolean) has been removed in 1.6 and
            // SearchRequest#templateSource(BytesReference) doesn't exist in 1.5, we are forced to use SearchRequest#templateSource(String)
            // that exist in both 1.5 and 1.6
            // TODO (2.0 upgrade): move back to BytesReference
            request.templateSource(builder.string());
        } else if (Strings.hasLength(requestPrototype.templateSource())) {
            // Here we convert watcher template into a ES core templates. Due to the different format we use, we
            // convert to the template format used in ES core
            BytesReference templateSource = requestPrototype.templateSource();
            try (XContentParser sourceParser = XContentFactory.xContent(templateSource).createParser(templateSource)) {
                sourceParser.nextToken();
                Template template = Template.parse(sourceParser);

                // Convert to the ES template format:
                XContentBuilder builder = jsonBuilder();
                builder.startObject();
                switch (template.getType()) {
                    case INDEXED:
                        builder.startObject("template");
                        builder.field("id", template.getTemplate());
                        builder.endObject();
                        break;
                    case FILE:
                        builder.startObject("template");
                        builder.field("file", template.getTemplate());
                        builder.endObject();
                        break;
                    case INLINE:
                        XContentHelper.writeRawField("template", new BytesArray(template.getTemplate()), builder, ToXContent.EMPTY_PARAMS);
                        break;
                }
                Map<String, Object> params = new HashMap<>();
                params.putAll(watcherContextParams);
                params.putAll(template.getParams());
                builder.field("params", params);
                builder.endObject();
                // Unfortunately because of SearchRequest#templateSource(BytesReference, boolean) has been removed in 1.6 and
                // SearchRequest#templateSource(BytesReference) doesn't exist in 1.5, we are forced to use SearchRequest#templateSource(String)
                // that exist in both 1.5 and 1.6
                // TODO (2.0 upgrade): move back to BytesReference
                request.templateSource(builder.string());
            }
        } else if (requestPrototype.templateName() != null) {
            // In Watcher templates on all places can be defined in one format
            // Can only be set via the Java api
            throw new WatcherException("SearchRequest#templateName() isn't supported, templates should be defined in the request body");
        }
        // falling back to an empty body
        return request;
    }


    /**
     * Reads a new search request instance for the specified parser.
     */
    public static SearchRequest readSearchRequest(XContentParser parser, SearchType searchType) throws IOException {
        BytesReference searchBody = null;
        String templateBody = null;
        IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
        SearchRequest searchRequest = new SearchRequest();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (INDICES_FIELD.match(currentFieldName)) {
                    List<String> indices = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            indices.add(parser.textOrNull());
                        } else {
                            throw new SearchRequestParseException("could not read search request. expected string values in [" + currentFieldName + "] field, but instead found [" + token + "]");
                        }
                    }
                    searchRequest.indices(indices.toArray(new String[indices.size()]));
                } else if (TYPES_FIELD.match(currentFieldName)) {
                    List<String> types = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            types.add(parser.textOrNull());
                        } else {
                            throw new SearchRequestParseException("could not read search request. expected string values in [" + currentFieldName + "] field, but instead found [" + token + "]");
                        }
                    }
                    searchRequest.types(types.toArray(new String[types.size()]));
                } else {
                    throw new SearchRequestParseException("could not read search request. unexpected array field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BODY_FIELD.match(currentFieldName)) {
                    XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
                    builder.copyCurrentStructure(parser);
                    searchBody = builder.bytes();
                } else if (INDICES_OPTIONS_FIELD.match(currentFieldName)) {
                    boolean expandOpen = DEFAULT_INDICES_OPTIONS.expandWildcardsOpen();
                    boolean expandClosed = DEFAULT_INDICES_OPTIONS.expandWildcardsClosed();
                    boolean allowNoIndices = DEFAULT_INDICES_OPTIONS.allowNoIndices();
                    boolean ignoreUnavailable = DEFAULT_INDICES_OPTIONS.ignoreUnavailable();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (EXPAND_WILDCARDS_FIELD.match(currentFieldName)) {
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
                                        throw new SearchRequestParseException("could not read search request. unknown value [" + parser.text() + "] for [" + currentFieldName + "] field ");
                                }
                            } else if (IGNORE_UNAVAILABLE_FIELD.match(currentFieldName)) {
                                ignoreUnavailable = parser.booleanValue();
                            } else if (ALLOW_NO_INDICES_FIELD.match(currentFieldName)) {
                                allowNoIndices = parser.booleanValue();
                            } else {
                                throw new SearchRequestParseException("could not read search request. unexpected index option [" + currentFieldName + "]");
                            }
                        } else {
                            throw new SearchRequestParseException("could not read search request. unexpected object field [" + currentFieldName + "]");
                        }
                    }
                    indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandOpen, expandClosed, DEFAULT_INDICES_OPTIONS);
                } else if (TEMPLATE_FIELD.match(currentFieldName)) {
                    XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
                    builder.copyCurrentStructure(parser);
                    templateBody = builder.string();
                } else {
                    throw new SearchRequestParseException("could not read search request. unexpected object field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (INDICES_FIELD.match(currentFieldName)) {
                    String indicesStr = parser.text();
                    searchRequest.indices(Strings.delimitedListToStringArray(indicesStr, ",", " \t"));
                } else if (TYPES_FIELD.match(currentFieldName)) {
                    String typesStr = parser.text();
                    searchRequest.types(Strings.delimitedListToStringArray(typesStr, ",", " \t"));
                } else if (SEARCH_TYPE_FIELD.match(currentFieldName)) {
                    searchType = SearchType.fromString(parser.text().toLowerCase(Locale.ROOT));
                    if (searchType == SearchType.SCAN){
                        throw new SearchRequestParseException("could not read search request. value [" + searchType.name() + "] is not supported for field [" + SEARCH_TYPE_FIELD.getPreferredName() + "]" );
                    }
                } else {
                    throw new SearchRequestParseException("could not read search request. unexpected string field [" + currentFieldName + "]");
                }
            } else {
                throw new SearchRequestParseException("could not read search request. unexpected token [" + token + "]");
            }
        }

        if (searchRequest.indices() == null) {
            searchRequest.indices(Strings.EMPTY_ARRAY);
        }
        searchRequest.searchType(searchType);
        searchRequest.indicesOptions(indicesOptions);
        if (searchBody != null) {
            // TODO (2.0 upgrade): move back to BytesReference instead of dealing with the array directly
            assert searchBody.hasArray();
            searchRequest.source(searchBody.array(), searchBody.arrayOffset(), searchBody.length());
        }
        if (templateBody != null) {
            // Unfortunately because of SearchRequest#templateSource(BytesReference, boolean) has been removed in 1.6 and
            // SearchRequest#templateSource(BytesReference) doesn't exist in 1.5, we are forced to use SearchRequest#templateSource(String)
            // that exist in both 1.5 and 1.6
            // TODO (2.0 upgrade): move back to BytesReference
            searchRequest.templateSource(templateBody);
        }
        return searchRequest;
    }

    /**
     * Writes the searchRequest to the specified builder.
     */
    public static XContentBuilder writeSearchRequest(SearchRequest searchRequest, XContentBuilder builder, ToXContent.Params params) throws IOException {
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
        if (Strings.hasLength(searchRequest.source())) {
            XContentHelper.writeRawField(BODY_FIELD.getPreferredName(), searchRequest.source(), builder, params);
        }
        if (Strings.hasLength(searchRequest.templateSource())) {
            XContentHelper.writeRawField(TEMPLATE_FIELD.getPreferredName(), searchRequest.templateSource(), builder, params);
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
