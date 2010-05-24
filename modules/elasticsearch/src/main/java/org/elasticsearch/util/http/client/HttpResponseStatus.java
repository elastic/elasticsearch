/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.elasticsearch.util.http.client;

import org.elasticsearch.util.http.url.Url;

/**
 * A class that represent the HTTP response' status line (code + text)
 */
public abstract class HttpResponseStatus<R> extends HttpContent<R> {

    public HttpResponseStatus(Url url, R response, AsyncHttpProvider<R> provider) {
        super(url, response, provider);
    }

    /**
     * Return the response status code
     *
     * @return the response status code
     */
    abstract public int getStatusCode();

    /**
     * Return the response status text
     *
     * @return the response status text
     */
    abstract public String getStatusText();

    /**
     * Protocol name from status line.
     *
     * @return Protocol name.
     */
    abstract public String getProtocolName();

    /**
     * Protocol major version.
     *
     * @return Major version.
     */
    abstract public int getProtocolMajorVersion();

    /**
     * Protocol minor version.
     *
     * @return Minor version.
     */
    abstract public int getProtocolMinorVersion();

    /**
     * Full protocol name + version
     *
     * @return protocol name + version
     */
    abstract public String getProtocolText();
}
