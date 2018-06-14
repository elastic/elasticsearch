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

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class RestResponse {

    private Map<String, List<String>> customHeaders;

    /**
     * The response content type.
     */
    public abstract String contentType();

    /**
     * The response content. Note, if the content is {@link org.elasticsearch.common.lease.Releasable} it
     * should automatically be released when done by the channel sending it.
     */
    public abstract BytesReference content();

    /**
     * The rest status code.
     */
    public abstract RestStatus status();

    public void copyHeaders(ElasticsearchException ex) {
        Set<String> headerKeySet = ex.getHeaderKeys();
        if (customHeaders == null) {
            customHeaders = new HashMap<>(headerKeySet.size());
        }
        for (String key : headerKeySet) {
            List<String> values = customHeaders.get(key);
            if (values == null) {
                values = new ArrayList<>();
                customHeaders.put(key, values);
            }
            values.addAll(ex.getHeader(key));
        }
    }

    /**
     * Add a custom header.
     */
    public void addHeader(String name, String value) {
        if (customHeaders == null) {
            customHeaders = new HashMap<>(2);
        }
        List<String> header = customHeaders.get(name);
        if (header == null) {
            header = new ArrayList<>();
            customHeaders.put(name, header);
        }
        header.add(value);
    }

    /**
     * Returns custom headers that have been added. This method should not be used to mutate headers.
     */
    public Map<String, List<String>> getHeaders() {
        if (customHeaders == null) {
            return Collections.emptyMap();
        } else {
            return customHeaders;
        }
    }
}
