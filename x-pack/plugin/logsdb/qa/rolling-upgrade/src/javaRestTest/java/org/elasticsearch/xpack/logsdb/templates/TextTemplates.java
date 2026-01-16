/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.templates;

import java.util.Arrays;

/**
 * Template configurations for text field type rolling upgrade tests.
 */
public final class TextTemplates {

    public static final String DATA_STREAM_NAME_PREFIX = "logs-text-bwc-test";

    public static final String TEMPLATE = """
        {
            "settings": {
              "index": {
                "mapping.source.mode": "synthetic"
              }
            },
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
                  "type": "text"
                }
              }
            }
        }""";

    public static final String TEMPLATE_WITH_MULTI_FIELD = """
        {
            "settings": {
              "index": {
                "mapping.source.mode": "synthetic"
              }
            },
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
                  "type": "text",
                  "fields": {
                    "kwd": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }
        }""";

    public static final String TEMPLATE_WITH_MULTI_FIELD_AND_IGNORE_ABOVE = """
        {
            "settings": {
              "index": {
                "mapping.source.mode": "synthetic"
              }
            },
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
                  "type": "text",
                  "fields": {
                    "kwd": {
                      "type": "keyword",
                      "ignore_above": 50
                    }
                  }
                }
              }
            }
        }""";

    public static Iterable<Object[]> templates() {
        return Arrays.asList(
            new Object[][] {
                { TEMPLATE, "basic" },
                { TEMPLATE_WITH_MULTI_FIELD, "with-keyword-multi-field" },
                { TEMPLATE_WITH_MULTI_FIELD_AND_IGNORE_ABOVE, "with-keyword-multi-field-and-ignore-above" } }
        );
    }
}
