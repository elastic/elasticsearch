/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.task;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.nio.charset.StandardCharsets;
import java.util.Set;

public final class DefaultEndpoints {

    public static final String DEFAULT_ELSER = ".elser-default";
    public static final Set<String> DEFAULT_IDS = Set.of(DEFAULT_ELSER);

    public static final BytesReference DEFAULT_ELSER_CONFIG = new BytesArray("""
        {
          "service": "elser",
          "service_settings": {
            "num_threads": 1,
            "adaptive_allocations": {
              "enabled": true,
              "min_number_of_allocations": 1
            }
          }
        }
        """);


    private DefaultEndpoints() {

    }
}
