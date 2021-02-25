/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.rollup.migration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.UpdateScript;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RollupMigrationScriptTests extends ESTestCase {

    private Script script;
    private ScriptService scriptService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        script = new Script(ScriptType.INLINE, RollupMigrationScript.ROLLUP_MIGRATION_SCRIPT, "", Collections.emptyMap());
        scriptService = new ScriptService(
            Settings.EMPTY,
            Map.of(RollupMigrationScript.ROLLUP_MIGRATION_SCRIPT, RollupMigrationScript.SCRIPT_ENGINE),
            Map.of(RollupMigrationScript.ROLLUP_MIGRATION_SCRIPT, RollupMigrationScript.SCRIPT_CONTEXT)
        );
    }

    public void testNothingTricky() {
        Map<String, Object> source = new HashMap<>();
        source.put("_rollup.id", "test_job");
        source.put("_rollup.version", 2);
        source.put("cab_color.terms._count", 1378719);
        source.put("cab_color.terms.value", null);
        source.put("passenger_count.avg._count", 1378719);
        source.put("passenger_count.avg.value", 2430845);
        source.put("pickup_datetime.date_histogram._count", 1378719);
        source.put("pickup_datetime.date_histogram.interval", "30d");
        source.put("pickup_datetime.date_histogram.time_zone", "UTC");
        source.put("pickup_datetime.date_histogram.timestamp", 1417824000000L);
        source.put("total_amount.max.value", 3006.35);
        source.put("total_amount.min.value", -250.3);
        source.put("total_amount.sum.value", 17283290.2);
        source.put("trip_distance.histogram._count", 1378719);
        source.put("trip_distance.histogram.interval", 10);
        source.put("trip_distance.histogram.value", 0);
        source.put("trip_type.terms._count", 1378719);
        source.put("trip_type.terms.value", null);

        Map<String, Object> context = new HashMap<>();
        Map<String, Object> params = new HashMap<>();

        context.put(SourceFieldMapper.NAME, source);

        UpdateScript.Factory factory = scriptService.compile(script, RollupMigrationScript.SCRIPT_CONTEXT);
        UpdateScript updateScript = factory.newInstance(params, context);
        updateScript.execute();

        Map<String, Object> actual = (Map) context.get(SourceFieldMapper.NAME);

        assertTrue(actual.containsKey("trip_type"));
        assertEquals(null, actual.get("trip_type"));
        assertTrue(actual.containsKey("trip_distance"));
        assertEquals(0, actual.get("trip_distance"));
        assertTrue(actual.containsKey("total_amount.max"));
        assertEquals(3006.35, actual.get("total_amount.max"));
        assertTrue(actual.containsKey("total_amount.min"));
        assertEquals(-250.3, actual.get("total_amount.min"));
        assertTrue(actual.containsKey("total_amount.sum"));
        assertEquals(17283290.2, actual.get("total_amount.sum"));
        assertTrue(actual.containsKey("pickup_datetime"));
        assertEquals(1417824000000L, actual.get("pickup_datetime"));
        assertTrue(actual.containsKey("passenger_count.count"));
        assertEquals(1378719, actual.get("passenger_count.count"));
        assertTrue(actual.containsKey("passenger_count.sum"));
        assertEquals(2430845, actual.get("passenger_count.sum"));
        assertTrue(actual.containsKey("cab_color"));
        assertEquals(null, actual.get("cab_color"));
        assertTrue(actual.containsKey("_doc_count"));
        assertEquals(1378719, actual.get("_doc_count"));
    }
}
