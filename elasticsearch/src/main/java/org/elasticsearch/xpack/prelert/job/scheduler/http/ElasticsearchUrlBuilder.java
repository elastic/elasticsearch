/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler.http;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

public class ElasticsearchUrlBuilder {

    private static final String SLASH = "/";
    private static final String COMMA = ",";
    private static final int SCROLL_CONTEXT_MINUTES = 60;
    private static final String INDEX_SETTINGS_END_POINT = "%s/_settings";
    private static final String SEARCH_SIZE_ONE_END_POINT = "_search?size=1";
    private static final String SEARCH_SCROLL_END_POINT = "_search?scroll=" + SCROLL_CONTEXT_MINUTES + "m&size=%d";
    private static final String CONTINUE_SCROLL_END_POINT = "_search/scroll?scroll=" + SCROLL_CONTEXT_MINUTES + "m";
    private static final String CLEAR_SCROLL_END_POINT = "_search/scroll";

    private final String baseUrl;
    private final String indexes;
    private final String types;

    private ElasticsearchUrlBuilder(String baseUrl, String indexes, String types) {
        this.baseUrl = Objects.requireNonNull(baseUrl);
        this.indexes = Objects.requireNonNull(indexes);
        this.types = Objects.requireNonNull(types);
    }

    public static ElasticsearchUrlBuilder create(String baseUrl, List<String> indexes, List<String> types) {
        String sanitisedBaseUrl = baseUrl.endsWith(SLASH) ? baseUrl : baseUrl + SLASH;
        String indexesAsString = indexes.stream().collect(Collectors.joining(COMMA));
        String typesAsString = types.stream().collect(Collectors.joining(COMMA));
        return new ElasticsearchUrlBuilder(sanitisedBaseUrl, indexesAsString, typesAsString);
    }

    public String buildIndexSettingsUrl(String index) {
        return newUrlBuilder().append(String.format(Locale.ROOT, INDEX_SETTINGS_END_POINT, index)).toString();
    }

    public String buildSearchSizeOneUrl() {
        return buildUrlWithIndicesAndTypes().append(SEARCH_SIZE_ONE_END_POINT).toString();
    }

    public String buildInitScrollUrl(int scrollSize) {
        return buildUrlWithIndicesAndTypes()
                .append(String.format(Locale.ROOT, SEARCH_SCROLL_END_POINT, scrollSize))
                .toString();
    }

    public String buildContinueScrollUrl() {
        return newUrlBuilder().append(CONTINUE_SCROLL_END_POINT).toString();
    }

    public String buildClearScrollUrl() {
        return newUrlBuilder().append(CLEAR_SCROLL_END_POINT).toString();
    }

    private StringBuilder newUrlBuilder() {
        return new StringBuilder(baseUrl);
    }

    private StringBuilder buildUrlWithIndicesAndTypes() {
        StringBuilder urlBuilder = buildUrlWithIndices();
        if (!types.isEmpty()) {
            urlBuilder.append(types).append(SLASH);
        }
        return urlBuilder;
    }

    private StringBuilder buildUrlWithIndices() {
        return newUrlBuilder().append(indexes).append(SLASH);
    }

    public String getBaseUrl() {
        return baseUrl;
    }
}
