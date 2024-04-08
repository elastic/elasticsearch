/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.client.NodeSelector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Represents a test fragment that contains the information needed to call an api
 */
public class ApiCallSection {

    private final String api;
    private final Map<String, String> params = new HashMap<>();
    private final Map<String, String> headers = new HashMap<>();
    private final List<Map<String, Object>> bodies = new ArrayList<>();
    private NodeSelector nodeSelector = NodeSelector.ANY;

    public ApiCallSection(String api) {
        this.api = api;
    }

    public String getApi() {
        return api;
    }

    public ApiCallSection copyWithNewApi(String api) {
        ApiCallSection copy = new ApiCallSection(api);
        for (var e : params.entrySet()) {
            copy.addParam(e.getKey(), e.getValue());
        }
        copy.addHeaders(headers);
        for (var b : bodies) {
            copy.addBody(b);
        }
        copy.nodeSelector = nodeSelector;
        return copy;
    }

    public Map<String, String> getParams() {
        // make sure we never modify the parameters once returned
        return unmodifiableMap(params);
    }

    public void addParam(String key, String value) {
        String existingValue = params.get(key);
        if (existingValue != null) {
            value = existingValue + "," + value;
        }
        this.params.put(key, value);
    }

    public void addHeaders(Map<String, String> otherHeaders) {
        this.headers.putAll(otherHeaders);
    }

    public Map<String, String> getHeaders() {
        return unmodifiableMap(headers);
    }

    public List<Map<String, Object>> getBodies() {
        return Collections.unmodifiableList(bodies);
    }

    public void addBody(Map<String, Object> body) {
        this.bodies.add(body);
    }

    public boolean hasBody() {
        return bodies.size() > 0;
    }

    /**
     * Selects the node on which to run this request.
     */
    public NodeSelector getNodeSelector() {
        return nodeSelector;
    }

    /**
     * Set the selector that decides which node can run this request.
     */
    public void setNodeSelector(NodeSelector nodeSelector) {
        this.nodeSelector = nodeSelector;
    }
}
