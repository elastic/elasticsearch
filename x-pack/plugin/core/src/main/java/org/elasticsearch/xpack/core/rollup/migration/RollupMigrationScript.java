/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.rollup.migration;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.UpdateScript;

import java.util.Map;
import java.util.Set;

public class RollupMigrationScript extends UpdateScript {

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
        // Rollup migration code goes here
    }

    private static class Factory implements UpdateScript.Factory {
        @Override
        public UpdateScript newInstance(Map<String, Object> params, Map<String, Object> ctx) {
            return new RollupMigrationScript(params, ctx);
        }
    }
}
