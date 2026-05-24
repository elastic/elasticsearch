/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;

public abstract class RequestParameters {

    public static final String INPUT = "input";

    protected final List<String> inputs;

    public RequestParameters(List<String> inputs) {
        this.inputs = Objects.requireNonNull(inputs);
    }

    Map<String, String> jsonParameters() {
        var additionalParameters = taskTypeParameters();
        var totalParameters = new HashMap<>(additionalParameters);
        totalParameters.put(INPUT, toJson(inputs, INPUT));

        return totalParameters;
    }

    protected Map<String, String> taskTypeParameters() {
        return Map.of();
    }
}
