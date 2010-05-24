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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.List;

/**
 * Represents the asynchronous HTTP response callback for an {@link org.elasticsearch.util.http.client.AsyncCompletionHandler}
 */
public interface Response {
    /**
     * Returns the status code for the request.
     *
     * @return The status code
     */
    public int getStatusCode();

    /**
     * Returns the status text for the request.
     *
     * @return The status text
     */
    public String getStatusText();

    /**
     * Returns an input stream for the response body. Note that you should not try to get this more than once,
     * and that you should not close the stream.
     *
     * @return The input stream
     * @throws java.io.IOException
     */
    public InputStream getResponseBodyAsStream() throws IOException;

    /**
     * Returns the first maxLength bytes of the response body as a string. Note that this does not check
     * whether the content type is actually a textual one, but it will use the charset if present in the content
     * type header.
     *
     * @param maxLength The maximum number of bytes to read
     * @return The response body
     * @throws java.io.IOException
     */
    public String getResponseBodyExcerpt(int maxLength) throws IOException;

    /**
     * Return the entire response body as a String.
     *
     * @return the entire response body as a String.
     * @throws IOException
     */
    public String getResponseBody() throws IOException;

    /**
     * Return the request {@link Url}. Note that if the request got redirected, the value of the {@link Url} will be
     * the last valid redirect url.
     *
     * @return the request {@link Url}.
     * @throws MalformedURLException
     */
    public Url getUrl() throws MalformedURLException;

    /**
     * Return the content-type header value.
     *
     * @return the content-type header value.
     */
    public String getContentType();

    /**
     * Return the response header
     *
     * @return the response header
     */
    public String getHeader(String name);

    /**
     * Return a {@link List} of the response header value.
     *
     * @return the response header
     */
    public List<String> getHeaders(String name);

    public Headers getHeaders();

    /**
     * Return true if the response redirects to another object.
     *
     * @return True if the response redirects to another object.
     */
    boolean isRedirected();

    /**
     * Subclasses SHOULD implement toString() in a way that identifies the request for logging.
     *
     * @return The textual representation
     */
    public String toString();

    /**
     * Return the list of {@link Cookie}.
     */
    public List<Cookie> getCookies();
}