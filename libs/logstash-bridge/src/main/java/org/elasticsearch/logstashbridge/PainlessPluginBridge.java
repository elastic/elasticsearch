/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;

import java.util.List;
import java.util.Map;

public class PainlessPluginBridge {
    private PainlessPluginBridge() {}

    public String painlessScriptEngineName() {
        return PainlessScriptEngine.NAME;
    }

    public PainlessScriptEngine newPainlessScriptEngine(Settings settings, Map<ScriptContext<?>, List<Whitelist>> contexts) {
        return new PainlessScriptEngine(settings, contexts);
    }

    public static List<Whitelist> getPainlessPluginBaseWhitelist() {
        return List.copyOf(PainlessPlugin.baseWhiteList());
    }
}
