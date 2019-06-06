/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_RESET_COOKIES;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;

public class HttpHandlingSettings {

    private final int maxContentLength;
    private final int maxChunkSize;
    private final int maxHeaderSize;
    private final int maxInitialLineLength;
    private final boolean resetCookies;
    private final boolean compression;
    private final int compressionLevel;
    private final boolean detailedErrorsEnabled;
    private final int pipeliningMaxEvents;
    private final long readTimeoutMillis;
    private boolean corsEnabled;

    public HttpHandlingSettings(int maxContentLength, int maxChunkSize, int maxHeaderSize, int maxInitialLineLength,
                                boolean resetCookies, boolean compression, int compressionLevel, boolean detailedErrorsEnabled,
                                int pipeliningMaxEvents, long readTimeoutMillis, boolean corsEnabled) {
        this.maxContentLength = maxContentLength;
        this.maxChunkSize = maxChunkSize;
        this.maxHeaderSize = maxHeaderSize;
        this.maxInitialLineLength = maxInitialLineLength;
        this.resetCookies = resetCookies;
        this.compression = compression;
        this.compressionLevel = compressionLevel;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.pipeliningMaxEvents = pipeliningMaxEvents;
        this.readTimeoutMillis = readTimeoutMillis;
        this.corsEnabled = corsEnabled;
    }

    public static HttpHandlingSettings fromSettings(Settings settings) {
        return new HttpHandlingSettings(Math.toIntExact(SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).getBytes()),
            Math.toIntExact(SETTING_HTTP_MAX_CHUNK_SIZE.get(settings).getBytes()),
            Math.toIntExact(SETTING_HTTP_MAX_HEADER_SIZE.get(settings).getBytes()),
            Math.toIntExact(SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings).getBytes()),
            SETTING_HTTP_RESET_COOKIES.get(settings),
            SETTING_HTTP_COMPRESSION.get(settings),
            SETTING_HTTP_COMPRESSION_LEVEL.get(settings),
            SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(settings),
            SETTING_PIPELINING_MAX_EVENTS.get(settings),
            SETTING_HTTP_READ_TIMEOUT.get(settings).getMillis(),
            SETTING_CORS_ENABLED.get(settings));
    }

    public int getMaxContentLength() {
        return maxContentLength;
    }

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public int getMaxHeaderSize() {
        return maxHeaderSize;
    }

    public int getMaxInitialLineLength() {
        return maxInitialLineLength;
    }

    public boolean isResetCookies() {
        return resetCookies;
    }

    public boolean isCompression() {
        return compression;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public boolean getDetailedErrorsEnabled() {
        return detailedErrorsEnabled;
    }

    public int getPipeliningMaxEvents() {
        return pipeliningMaxEvents;
    }

    public long getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public boolean isCorsEnabled() {
        return corsEnabled;
    }
}
