/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;

import java.util.Map;
import java.util.function.LongSupplier;

public class ScriptBridge {
    private ScriptBridge() {}

    public static ScriptService newScriptService(
        Settings settings,
        Map<String, ScriptEngine> engines,
        Map<String, ScriptContext<?>> contexts,
        LongSupplier timeProvider
    ) {
        return new ScriptService(settings, engines, contexts, timeProvider);
    }

    public static Map<String, ScriptContext<?>> coreScriptContexts() {
        return Map.copyOf(ScriptModule.CORE_CONTEXTS);
    }
}
