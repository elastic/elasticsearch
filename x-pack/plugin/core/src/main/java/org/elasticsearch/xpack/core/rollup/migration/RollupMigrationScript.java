/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.rollup.migration;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.UpdateScript;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RollupMigrationScript extends UpdateScript {

    /*
              "_rollup.id": "test_job"
                  "_rollup.version": 2,
                  "cab_color.terms._count": 1378719,
                  "cab_color.terms.value": null,
                  "passenger_count.avg._count": 1378719,
                  "passenger_count.avg.value": 2430845,
                  "pickup_datetime.date_histogram._count": 1378719,
                  "pickup_datetime.date_histogram.interval": "30d",
                  "pickup_datetime.date_histogram.time_zone": "UTC",
                  "pickup_datetime.date_histogram.timestamp": 1417824000000,
                  "total_amount.max.value": 3006.35,
                  "total_amount.min.value": -250.3,
                  "total_amount.sum.value": 17283290.2,
                  "trip_distance.histogram._count": 1378719,
                  "trip_distance.histogram.interval": 10,
                  "trip_distance.histogram.value": 0,
                  "trip_type.terms._count": 1378719,
                  "trip_type.terms.value": null,

     */

    private enum MigrationPatterns {
        METRIC_MAX("(\\w+\\.max)\\.value"),
        METRIC_MIN("(\\w+\\.min)\\.value"),
        METRIC_SUM("(\\w+\\.sum)\\.value"),
        METRIC_AVG_COUNT("(\\w+)\\.avg\\._count") {
            @Override
            public String migrate(String field) {
                Matcher matcher = getPattern().matcher(field);
                if (matcher.matches()) {
                    return matcher.group(1) + ".count";
                }
                return null;
            }
        },
        METRIC_AVG_SUM("(\\w+)\\.avg\\.value") {
            @Override
            public String migrate(String field) {
                Matcher matcher = getPattern().matcher(field);
                if (matcher.matches()) {
                    return matcher.group(1) + ".sum";
                }
                return null;
            }
        },
        DATE_HISTOGRAM_TIMESTAMP("(\\w+)\\.date_histogram\\.timestamp"),
        DATE_HISTOGRAM_COUNT("(\\w+)\\.date_histogram\\._count") {
            @Override
            public String migrate(String field) {
                Matcher matcher = getPattern().matcher(field);
                if (matcher.matches()) {
                    return "_doc_count";
                }
                return null;
            }
        },
        TERMS_VALUE("(\\w+)\\.terms\\.value"),
        HISTOGRAM_VALUE("(\\w+)\\.histogram\\.value");

        private final Pattern fieldPattern;

        /**
         * Given an input field name, return the migrated name for that field, or null.
         */
        public String migrate(String field) {
            Matcher matcher = getPattern().matcher(field);
            if (matcher.matches()) {
                return matcher.group(1);
            }
            return null;
        }

        public Pattern getPattern() {
            return fieldPattern;
        }

        MigrationPatterns(String pattern) {
            this.fieldPattern = Pattern.compile(pattern);
        }
    }

    public static final String ROLLUP_MIGRATION_SCRIPT = "RollupMigrationScript";
    public static final ScriptContext<Factory> SCRIPT_CONTEXT = new ScriptContext<>(
        ROLLUP_MIGRATION_SCRIPT,
        Factory.class,
        1,
        TimeValue.MAX_VALUE,
        new Tuple<>(100, TimeValue.MAX_VALUE)
    );

    public static final ScriptEngine SCRIPT_ENGINE = new ScriptEngine() {
        @Override
        public String getType() {
            return ROLLUP_MIGRATION_SCRIPT;
        }

        @Override
        public <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> context, Map<String, String> params) {
            return (FactoryType) new Factory();
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Set.of(SCRIPT_CONTEXT);
        }
    };

    public RollupMigrationScript(Map<String, Object> params, Map<String, Object> ctx) {
        super(params, ctx);
    }

    @Override
    public void execute() {
        Map<String, Object> context = getCtx();
        Map<String, Object> originalSource = (Map) context.get(SourceFieldMapper.NAME);
        Map<String, Object> newSource = new HashMap<>(originalSource.size());
        for (Map.Entry<String, Object> sourceEntry : originalSource.entrySet()) {
            for (MigrationPatterns migrationPattern : MigrationPatterns.values()) {
                String newFieldName = migrationPattern.migrate(sourceEntry.getKey());
                if (newFieldName != null) {
                    newSource.put(newFieldName, sourceEntry.getValue());
                }
            }
        }
        context.put(SourceFieldMapper.NAME, newSource);
    }

    private static class Factory implements UpdateScript.Factory {
        @Override
        public UpdateScript newInstance(Map<String, Object> params, Map<String, Object> ctx) {
            return new RollupMigrationScript(params, ctx);
        }
    }
}
