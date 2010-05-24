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

import org.elasticsearch.util.http.collection.Pair;

import java.util.*;

public class Headers implements Iterable<Pair<String, String>> {
    public static final String CONTENT_TYPE = "Content-Type";

    private List<Pair<String, String>> headers = new ArrayList<Pair<String, String>>();

    public static Headers unmodifiableHeaders(Headers headers) {
        return new UnmodifiableHeaders(headers);
    }

    public Headers() {
    }

    public Headers(Headers src) {
        if (src != null) {
            for (Pair<String, String> header : src) {
                add(header);
            }
        }
    }

    public Headers(Map<String, Collection<String>> headers) {
        for (Map.Entry<String, Collection<String>> entry : headers.entrySet()) {
            for (String value : entry.getValue()) {
                add(entry.getKey(), value);
            }
        }
    }

    /**
     * Adds the specified header and returns this headers object.
     *
     * @param name  The header name
     * @param value The header value
     * @return This object
     */
    public Headers add(String name, String value) {
        headers.add(new Pair<String, String>(name, value));
        return this;
    }

    /**
     * Adds the specified header and returns this headers object.
     *
     * @param header The name / value pair
     * @return This object
     */
    public Headers add(Pair<String, String> header) {
        headers.add(new Pair<String, String>(header.getFirst(), header.getSecond()));
        return this;
    }

    /**
     * Adds all headers from the given headers object to this object and returns this headers object.
     *
     * @param srcHeaders The source headers object
     * @return This object
     */
    public Headers addAll(Headers srcHeaders) {
        for (Pair<String, String> entry : srcHeaders.headers) {
            headers.add(new Pair<String, String>(entry.getFirst(), entry.getSecond()));
        }
        return this;
    }

    /**
     * Convenience method to add a Content-type header
     *
     * @param contentType content type to set
     * @return This object
     */
    public Headers addContentTypeHeader(String contentType) {
        return add(CONTENT_TYPE, contentType);
    }

    /**
     * Replaces all existing headers with the header given.
     *
     * @param header The header name.
     * @param value  The new header value.
     */
    public void replace(final String header, final String value) {
        remove(header);
        add(header, value);
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<Pair<String, String>> iterator() {
        return headers.iterator();
    }

    /**
     * Returns the value of first header of the given name.
     *
     * @param name The header's name
     * @return The value
     */
    public String getHeaderValue(String name) {
        for (Pair<String, String> header : this) {
            if (name.equalsIgnoreCase(header.getFirst())) {
                return header.getSecond();
            }
        }
        return null;
    }

    /**
     * Returns the values of all header of the given name.
     *
     * @param name The header name
     * @return The values, will not be <code>null</code>
     */
    public List<String> getHeaderValues(String name) {
        ArrayList<String> values = new ArrayList<String>();

        for (Pair<String, String> header : this) {
            if (name.equalsIgnoreCase(header.getFirst())) {
                values.add(header.getSecond());
            }
        }
        return values;
    }

    /**
     * Adds the specified header(s) and returns this headers object.
     *
     * @param name The header name
     * @return This object
     */
    public Headers remove(String name) {
        for (Iterator<Pair<String, String>> it = headers.iterator(); it.hasNext();) {
            Pair<String, String> header = it.next();

            if (name.equalsIgnoreCase(header.getFirst())) {
                it.remove();
            }
        }
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final Headers other = (Headers) obj;
        if (headers == null) {
            if (other.headers != null)
                return false;
        } else if (!headers.equals(other.headers))
            return false;
        return true;
    }

    private static class UnmodifiableHeaders extends Headers {
        final Headers headers;

        UnmodifiableHeaders(Headers headers) {
            this.headers = headers;
        }

        @Override
        public Headers add(Pair<String, String> header) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Headers add(String name, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Headers addAll(Headers srcHeaders) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Headers addContentTypeHeader(String contentType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            return headers.equals(obj);
        }

        @Override
        public String getHeaderValue(String name) {
            return headers.getHeaderValue(name);
        }

        @Override
        public List<String> getHeaderValues(String name) {
            return headers.getHeaderValues(name);
        }

        @Override
        public Iterator<Pair<String, String>> iterator() {
            return headers.iterator();
        }

        @Override
        public Headers remove(String name) {
            throw new UnsupportedOperationException();
        }
    }
}
