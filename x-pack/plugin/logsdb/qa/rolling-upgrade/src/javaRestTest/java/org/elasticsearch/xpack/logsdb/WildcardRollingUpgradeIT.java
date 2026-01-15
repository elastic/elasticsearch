/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.Mapper;

import java.util.Arrays;

public class WildcardRollingUpgradeIT extends AbstractStringWithIgnoreAboveRollingUpgradeTestCase {

    private static final String DATA_STREAM_NAME_PREFIX = "logs-wildcard-bwc-test";

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
                  "type": "wildcard"
                }
              }
            }
        }""";

    private static final String TEMPLATE_WITH_IGNORE_ABOVE = """
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
                  "type": "wildcard",
                  "ignore_above": 50
                }
              }
            }
        }""";

    public WildcardRollingUpgradeIT(String template, String testScenario, Mapper.IgnoreAbove ignoreAbove) {
        super(DATA_STREAM_NAME_PREFIX + "." + testScenario, template, ignoreAbove);
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[][] {
                { TEMPLATE, "basic", new Mapper.IgnoreAbove(null, IndexMode.LOGSDB) },
                { TEMPLATE_WITH_IGNORE_ABOVE, "with-ignore-above", new Mapper.IgnoreAbove(50, IndexMode.LOGSDB) } }
        );
    }
}
