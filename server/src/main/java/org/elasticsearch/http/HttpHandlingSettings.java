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

public class HttpHandlingSettings {

    private final int maxContentLength;
    private final int maxChunkSize;
    private final int maxHeaderSize;
    private final int maxInitialLineLength;
    private final boolean resetCookies;
    private final boolean compression;
    private final int compressionLevel;
    private final boolean detailedErrorsEnabled;

    public HttpHandlingSettings(int maxContentLength, int maxChunkSize, int maxHeaderSize, int maxInitialLineLength,
                                boolean resetCookies, boolean compression, int compressionLevel, boolean detailedErrorsEnabled) {
        this.maxContentLength = maxContentLength;
        this.maxChunkSize = maxChunkSize;
        this.maxHeaderSize = maxHeaderSize;
        this.maxInitialLineLength = maxInitialLineLength;
        this.resetCookies = resetCookies;
        this.compression = compression;
        this.compressionLevel = compressionLevel;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
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
}
