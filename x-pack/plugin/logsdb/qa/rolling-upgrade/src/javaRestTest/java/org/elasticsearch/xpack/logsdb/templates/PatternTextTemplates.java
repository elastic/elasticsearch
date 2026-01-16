/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.templates;

import java.util.Arrays;

/**
 * Template configurations for pattern_text field type rolling upgrade tests.
 */
public final class PatternTextTemplates {

    public static final String DATA_STREAM_NAME_PREFIX = "logs-pattern-text-bwc-test";

    public static final String TEMPLATE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "length": {
                  "type": "long"
                },
                "factor": {
                  "type": "double"
                },
                "message": {
                  "type": "pattern_text"
                }
              }
            }
        }""";

    public static Iterable<Object[]> templates() {
        return Arrays.asList(new Object[][] { { TEMPLATE, "basic" } });
    }
}
