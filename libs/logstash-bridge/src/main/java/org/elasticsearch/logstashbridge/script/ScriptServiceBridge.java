/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.script;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.common.SettingsBridge;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public class ScriptServiceBridge extends StableBridgeAPI.Proxy<ScriptService> implements Closeable {
    public ScriptServiceBridge wrap(final ScriptService delegate) {
        return new ScriptServiceBridge(delegate);
    }

    public ScriptServiceBridge(final SettingsBridge settingsBridge, final LongSupplier timeProvider) {
        super(getScriptService(settingsBridge.unwrap(), timeProvider));
    }

    public ScriptServiceBridge(ScriptService delegate) {
        super(delegate);
    }

    private static ScriptService getScriptService(final Settings settings, final LongSupplier timeProvider) {
        final List<Whitelist> painlessBaseWhitelist = getPainlessBaseWhiteList();
        final Map<ScriptContext<?>, List<Whitelist>> scriptContexts = Map.of(
            IngestScript.CONTEXT,
            painlessBaseWhitelist,
            IngestConditionalScript.CONTEXT,
            painlessBaseWhitelist
        );
        final Map<String, ScriptEngine> scriptEngines = Map.of(
            PainlessScriptEngine.NAME,
            new PainlessScriptEngine(settings, scriptContexts),
            MustacheScriptEngine.NAME,
            new MustacheScriptEngine(settings)
        );
        return new ScriptService(settings, scriptEngines, ScriptModule.CORE_CONTEXTS, timeProvider);
    }

    private static List<Whitelist> getPainlessBaseWhiteList() {
        return PainlessPlugin.baseWhiteList();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
