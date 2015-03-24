/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import static org.elasticsearch.watcher.support.WatcherDateUtils.formatDate;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public final class WatcherUtils {

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
                switch (currentFieldName) {
                    case "indices":
                        List<String> indices = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                indices.add(parser.textOrNull());
                            } else {
                                throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
                            }
                        }
                        searchRequest.indices(indices.toArray(new String[indices.size()]));
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                XContentBuilder builder;
                switch (currentFieldName) {
                    case "body":
                        builder = XContentBuilder.builder(parser.contentType().xContent());
                        builder.copyCurrentStructure(parser);
                        searchRequest.source(builder);
                        break;
                    case "indices_options":
                        boolean expandOpen = DEFAULT_INDICES_OPTIONS.expandWildcardsOpen();
                        boolean expandClosed = DEFAULT_INDICES_OPTIONS.expandWildcardsClosed();
                        boolean allowNoIndices = DEFAULT_INDICES_OPTIONS.allowNoIndices();
                        boolean ignoreUnavailable = DEFAULT_INDICES_OPTIONS.ignoreUnavailable();

                        String indicesFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                indicesFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                switch (indicesFieldName) {
                                    case "expand_wildcards":
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
                                                throw new ElasticsearchIllegalArgumentException("Unexpected value [" + parser.text() + "]");
                                        }
                                        break;
                                    case "ignore_unavailable":
                                        ignoreUnavailable = parser.booleanValue();
                                        break;
                                    case "allow_no_indices":
                                        allowNoIndices = parser.booleanValue();
                                        break;
                                    default:
                                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + indicesFieldName + "]");
                                }
                            } else {
                                throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
                            }
                        }
                        indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandOpen, expandClosed, DEFAULT_INDICES_OPTIONS);
                        break;
                    case "template":
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                switch (currentFieldName) {
                                    case "name":
                                        searchRequest.templateName(parser.textOrNull());
                                        break;
                                    case "type":
                                        try {
                                            searchRequest.templateType(ScriptService.ScriptType.valueOf(parser.text().toUpperCase(Locale.ROOT)));
                                        } catch (IllegalArgumentException iae) {
                                            throw new WatcherSettingsException("could not parse search request. unknown template type [" + parser.text() + "]");
                                        }
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                if ("params".equals(currentFieldName)) {
                                    searchRequest.templateParams(flattenModel(parser.map()));
                                }
                            }
                        }
                        break;

                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "search_type":
                        searchType = SearchType.fromString(parser.text().toLowerCase(Locale.ROOT));
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
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
            builder.field("search_type", searchRequest.searchType().toString().toLowerCase(Locale.ENGLISH));
        }
        if (searchRequest.indices() != null) {
            builder.array("indices", searchRequest.indices());
        }
        if (Strings.hasLength(searchRequest.source())) {
            XContentHelper.writeRawField("body", searchRequest.source(), builder, params);
        }
        if (searchRequest.templateName() != null) {
            builder.startObject("template")
                    .field("name", searchRequest.templateName());
            if (searchRequest.templateType() != null) {
                builder.field("type", searchRequest.templateType().name().toLowerCase(Locale.ROOT));
            }
            if (searchRequest.templateParams() != null && !searchRequest.templateParams().isEmpty()) {
                builder.field("params", searchRequest.templateParams());
            }
            builder.endObject();
        }

        if (searchRequest.indicesOptions() != DEFAULT_INDICES_OPTIONS) {
            IndicesOptions options = searchRequest.indicesOptions();
            builder.startObject("indices_options");
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
            builder.field("expand_wildcards", value);
            builder.field("ignore_unavailable", options.ignoreUnavailable());
            builder.field("allow_no_indices", options.allowNoIndices());
            builder.endObject();
        }
        return builder.endObject();
    }

    public static Map<String, String> flattenModel(Map<String, Object> map) {
        Map<String, String> result = new HashMap<>();
        flattenModel("", map, result);
        return result;
    }

    private static void flattenModel(String key, Object value, Map<String, String> result) {
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
