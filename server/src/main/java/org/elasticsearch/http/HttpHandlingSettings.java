/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_RESET_COOKIES;

public record HttpHandlingSettings(
    int maxContentLength,
    int maxChunkSize,
    int maxHeaderSize,
    int maxInitialLineLength,
    boolean resetCookies,
    boolean compression,
    int compressionLevel,
    boolean detailedErrorsEnabled
) {

    public static HttpHandlingSettings fromSettings(Settings settings) {
        return new HttpHandlingSettings(
            Math.toIntExact(SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).getBytes()),
            Math.toIntExact(SETTING_HTTP_MAX_CHUNK_SIZE.get(settings).getBytes()),
            Math.toIntExact(SETTING_HTTP_MAX_HEADER_SIZE.get(settings).getBytes()),
            Math.toIntExact(SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings).getBytes()),
            SETTING_HTTP_RESET_COOKIES.get(settings),
            SETTING_HTTP_COMPRESSION.get(settings),
            SETTING_HTTP_COMPRESSION_LEVEL.get(settings),
            SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(settings)
        );
    }
}
