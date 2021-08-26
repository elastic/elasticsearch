/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;

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
     * The response content. Note, if the content is {@link Releasable} it
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

    public Map<String, List<String>> filterHeaders(Map<String, List<String>> headers) {
        return headers;
    }
}
