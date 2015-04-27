/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.watcher.WatcherException;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.WatcherDateUtils.formatDate;

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
    static final ParseField TEMPLATE_NAME_FIELD = new ParseField("name");
    static final ParseField TEMPLATE_TYPE_FIELD = new ParseField("type");
    static final ParseField TEMPLATE_PARAMS_FIELD = new ParseField("params");

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

    /**
     * Reads a new search request instance for the specified parser.
     */
    public static SearchRequest readSearchRequest(XContentParser parser, SearchType searchType) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
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
                XContentBuilder builder;
                if (BODY_FIELD.match(currentFieldName)) {
                    builder = XContentBuilder.builder(parser.contentType().xContent());
                    builder.copyCurrentStructure(parser);
                    searchRequest.source(builder);
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
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if (TEMPLATE_NAME_FIELD.match(currentFieldName)) {
                                searchRequest.templateName(parser.textOrNull());
                            } else if (TEMPLATE_TYPE_FIELD.match(currentFieldName)) {
                                try {
                                    searchRequest.templateType(ScriptService.ScriptType.valueOf(parser.text().toUpperCase(Locale.ROOT)));
                                } catch (IllegalArgumentException iae) {
                                    throw new SearchRequestParseException("could not parse search request. unknown template type [" + parser.text() + "]");
                                }
                            } else {
                                throw new SearchRequestParseException("could not read search request. unexpected template field [" + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("params".equals(currentFieldName)) {
                                searchRequest.templateParams(flattenModel(parser.map()));
                            }
                        } else {
                            throw new SearchRequestParseException("could not read search request. unexpected template token [" + token + "]");
                        }
                    }
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
        if (searchRequest.templateName() != null) {
            builder.startObject(TEMPLATE_FIELD.getPreferredName())
                    .field(TEMPLATE_NAME_FIELD.getPreferredName(), searchRequest.templateName());
            if (searchRequest.templateType() != null) {
                builder.field(TEMPLATE_TYPE_FIELD.getPreferredName(), searchRequest.templateType().name().toLowerCase(Locale.ROOT));
            }
            if (searchRequest.templateParams() != null && !searchRequest.templateParams().isEmpty()) {
                builder.field(TEMPLATE_PARAMS_FIELD.getPreferredName(), searchRequest.templateParams());
            }
            builder.endObject();
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
