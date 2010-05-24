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
 */
package org.elasticsearch.util.http.client;

import org.elasticsearch.util.http.url.Url;

/**
 * A callback class used when an HTTP response body is received.
 */
public abstract class HttpResponseBodyPart<R> extends HttpContent<R> {

    public HttpResponseBodyPart(Url url, R response, AsyncHttpProvider<R> provider) {
        super(url, response, provider);
    }

    /**
     * Return the response body's part bytes received.
     *
     * @return the response body's part bytes received.
     */
    abstract public byte[] getBodyPartBytes();

}
