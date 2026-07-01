/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.script.ScriptException;

import java.util.Map;

/**
 * Base for allocation-tracking tests. Each test compiles a script for the test context under a chosen per-context byte limit
 * and then either inspects the running allocation total or asserts that the limit was tripped. Because the limit must vary
 * between tests, a fresh engine is built per {@link #compile} call rather than reusing the shared one from
 * {@link ScriptTestCase}; the {@link #scriptContexts()} whitelist setup is inherited unchanged.
 */
public abstract class AllocationTestCase extends ScriptTestCase {

    /** Affix-setting key carrying the allocation byte limit for the test context. */
    protected static final String LIMIT_KEY = "script.painless.max_allocation_bytes.context." + PainlessTestScript.CONTEXT.name + ".limit";

    @Override
    protected PainlessScriptEngine buildScriptEngine() {
        // Each test builds its own engine via compile(source, limit) to set a per-test limit, so the shared engine is unused.
        return null;
    }

    /** Compiles {@code source} for the test context under {@code limit} and returns a fresh script instance. */
    protected PainlessTestScript compile(String source, String limit) {
        Settings settings = Settings.builder().put(LIMIT_KEY, limit).build();
        PainlessScriptEngine engine = new PainlessScriptEngine(settings, scriptContexts());
        PainlessTestScript.Factory factory = engine.compile("test", source, PainlessTestScript.CONTEXT, Map.of());
        return factory.newInstance(Map.of());
    }

    /** Runs {@code source} under a 1mb limit and returns the running allocation total afterwards. */
    protected long allocatedBytes(String source) {
        PainlessTestScript script = compile(source, "1mb");
        script.execute();
        return ((PainlessScript) script).getAllocBytes();
    }

    /** Asserts that running {@code source} under a 1b limit trips the allocation limit. */
    protected void assertTripsLimit(String source) {
        PainlessTestScript script = compile(source, "1b");
        ScriptException e = expectThrows(ScriptException.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error for [" + source + "], but got: " + e, e);
    }
}
