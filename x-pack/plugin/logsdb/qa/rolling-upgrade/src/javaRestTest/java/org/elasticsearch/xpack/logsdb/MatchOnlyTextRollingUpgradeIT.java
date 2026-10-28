/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.index.mapper.MapperFeatures;

import java.util.List;

public class MatchOnlyTextRollingUpgradeIT extends AbstractStringTypeLogsdbRollingUpgradeTestCase {

    private static final String DATA_STREAM_NAME_PREFIX = "logs-match-only-text-bwc-test";

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
                  "type": "match_only_text"
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
                  "type": "match_only_text",
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
                  "type": "match_only_text",
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

    @Override
    protected List<TemplateConfig> getTemplates() {
        return List.of(
            new TemplateConfig(DATA_STREAM_NAME_PREFIX + ".basic", TEMPLATE),
            new TemplateConfig(DATA_STREAM_NAME_PREFIX + ".with-keyword-multi-field", TEMPLATE_WITH_MULTI_FIELD),
            new TemplateConfig(
                DATA_STREAM_NAME_PREFIX + ".with-keyword-multi-field-and-ignore-above",
                TEMPLATE_WITH_MULTI_FIELD_AND_IGNORE_ABOVE
            )
        );
    }

    @Override
    protected void checkRequiredFeatures() {
        assumeTrue(
            "Match only text block loader bug is present and fix is not present in this cluster",
            oldClusterHasFeature(MapperFeatures.MATCH_ONLY_TEXT_BLOCK_LOADER_FIX)
        );
    }

}
