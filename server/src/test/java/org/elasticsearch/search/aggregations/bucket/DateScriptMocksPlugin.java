/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Mock scripts shared by DateRangeIT and DateHistogramIT.
 *
 * Provides {@link DateScriptMocksPlugin#EXTRACT_FIELD}, {@link DateScriptMocksPlugin#DOUBLE_PLUS_ONE_MONTH},
 * and {@link DateScriptMocksPlugin#LONG_PLUS_ONE_MONTH} scripts.
 */
public class DateScriptMocksPlugin extends MockScriptPlugin {
    static final String EXTRACT_FIELD = "extract_field";
    static final String DOUBLE_PLUS_ONE_MONTH = "double_date_plus_1_month";
    static final String LONG_PLUS_ONE_MONTH = "long_date_plus_1_month";
    static final String CURRENT_DATE = "current_date";

    @Override
    public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put(EXTRACT_FIELD, params -> {
            LeafDocLookup docLookup = (LeafDocLookup) params.get("doc");
            String fieldname = (String) params.get("fieldname");
            return docLookup.get(fieldname);
        });
        scripts.put(DOUBLE_PLUS_ONE_MONTH, params -> {
            Instant instant = Instant.ofEpochMilli(Double.valueOf((double) params.get("_value")).longValue());
            return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).plusMonths(1).toInstant().toEpochMilli();
        });
        scripts.put(LONG_PLUS_ONE_MONTH, params -> {
            Instant instant = Instant.ofEpochMilli((long) params.get("_value"));
            return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).plusMonths(1).toInstant().toEpochMilli();
        });
        return scripts;
    }

    @Override
    protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put(CURRENT_DATE, params -> ZonedDateTime.now().toInstant().toEpochMilli());
        return scripts;
    }
}
