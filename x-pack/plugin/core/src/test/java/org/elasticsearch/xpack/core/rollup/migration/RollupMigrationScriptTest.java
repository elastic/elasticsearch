/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.rollup.migration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.UpdateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RollupMigrationScriptTest extends ESTestCase {

    public void testEmptyDocument() {
        Script script = new Script(ScriptType.INLINE, RollupMigrationScript.ROLLUP_MIGRATION_SCRIPT, "", Collections.emptyMap());
        ScriptService scriptService = new ScriptService(Settings.EMPTY,
            Map.of(RollupMigrationScript.ROLLUP_MIGRATION_SCRIPT, RollupMigrationScript.SCRIPT_ENGINE),
            Map.of(RollupMigrationScript.ROLLUP_MIGRATION_SCRIPT, RollupMigrationScript.SCRIPT_CONTEXT));
        Map<String, Object> context = new HashMap<>();
        Map<String, Object> params = new HashMap<>();

        UpdateScript.Factory factory = scriptService.compile(script, RollupMigrationScript.SCRIPT_CONTEXT);
        UpdateScript updateScript = factory.newInstance(params, context);
        updateScript.execute();
    }

}
