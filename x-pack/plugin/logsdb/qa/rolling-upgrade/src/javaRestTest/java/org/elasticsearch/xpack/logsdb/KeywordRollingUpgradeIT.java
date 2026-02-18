/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.Mapper;

import java.util.Map;

public class KeywordRollingUpgradeIT extends AbstractStringWithIgnoreAboveRollingUpgradeTestCase {

    private static final String DATA_STREAM_NAME_PREFIX = "logs-keyword-bwc-test";

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
                  "type": "keyword"
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
                  "type": "keyword",
                  "ignore_above": 50
                }
              }
            }
        }""";

    @Override
    protected Map<String, TemplateConfigWithIgnoreAbove> getTemplatesWithIgnoreAbove() {
        var basicTemplate = new TemplateConfigWithIgnoreAbove(
            DATA_STREAM_NAME_PREFIX + ".basic",
            TEMPLATE,
            new Mapper.IgnoreAbove(null, IndexMode.LOGSDB)
        );
        var templateWithIgnoreAbove = new TemplateConfigWithIgnoreAbove(
            DATA_STREAM_NAME_PREFIX + ".with-ignore-above",
            TEMPLATE_WITH_IGNORE_ABOVE,
            new Mapper.IgnoreAbove(50, IndexMode.LOGSDB)
        );

        return Map.of(basicTemplate.dataStreamName(), basicTemplate, templateWithIgnoreAbove.dataStreamName(), templateWithIgnoreAbove);
    }
}
