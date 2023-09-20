/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record HttpResponse(Map<String, List<String>> headers, byte[] body) {
    public HttpResponse {
        Objects.requireNonNull(headers);
        Objects.requireNonNull(body);
    }
}
