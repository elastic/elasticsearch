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

package org.elasticsearch.plugins.repository;

import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

public interface PluginDownloader {

    /**
     * Downloads a file from an URL.
     *
     * @param source the source url
     * @return the destination of the downloaded file
     */
    Path download(String source) throws IOException;

    /**
     * Adds a listener to the plugin downloader
     *
     * @param listener the listener used to monitor the download
     */
    void addListener(Listener listener);

    /**
     * Listener used to monitor a download
     */
    public interface Listener {

        /**
         * Called when a download starts
         *
         * @param url the url of the download
         */
        void onDownloadBegin(String url);

        /**
         * Called when a chunk of the file is downloaded
         *
         * @param url        the url of the download
         * @param total      the total number in bytes of the file to download, or -1 if the length is unknown
         * @param downloaded the number of bytes already downloaded
         */
        void onDownloadTick(String url, long total, long downloaded);

        /**
         * Called when a download succeed
         *
         * @param url  the url of the download
         * @param dest the destination where to save the downloaded file
         */
        void onDownloadEnd(String url, Path dest);

        /**
         * Called when the download times out
         *
         * @param url     the url of the download
         * @param timeout the time out value
         */
        void onDownloadTimeout(String url, TimeValue timeout);

        /**
         * Called when a redirection is detected
         *
         * @param url         the url of the download
         * @param redirection the new url
         */
        void onDownloadRedirection(String url, URL redirection);

        /**
         * Call when the download ends in error
         *
         * @param url       the url of the download
         * @param exception the exception, if any
         */
        void onDownloadError(String url, Exception exception);
    }
}
