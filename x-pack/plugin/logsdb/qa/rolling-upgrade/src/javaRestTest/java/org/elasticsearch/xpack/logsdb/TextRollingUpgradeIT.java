/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.Arrays;

public class TextRollingUpgradeIT extends AbstractStringTypeLogsdbRollingUpgradeTestCase {

    private static final String DATA_STREAM_NAME_PREFIX = "logs-text-bwc-test";

    private static final String TEMPLATE = """
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
                  "type": "text"
                }
              }
            }
        }""";

    private static final String TEMPLATE_WITH_MULTI_FIELD = """
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

    private static final String TEMPLATE_WITH_MULTI_FIELD_AND_IGNORE_ABOVE = """
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

    public TextRollingUpgradeIT(String template, String testScenario) {
        super(DATA_STREAM_NAME_PREFIX + "." + testScenario, template);
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[][] {
                { TEMPLATE, "basic" },
                { TEMPLATE_WITH_MULTI_FIELD, "with-keyword-multi-field" },
                { TEMPLATE_WITH_MULTI_FIELD_AND_IGNORE_ABOVE, "with-keyword-multi-field-and-ignore-above" } }
        );
    }

}
