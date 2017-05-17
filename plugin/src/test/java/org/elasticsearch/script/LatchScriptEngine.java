/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.script;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LatchScriptEngine implements ScriptEngine {

    private static final String NAME = "latch";
    private static final LatchScriptEngine INSTANCE = new LatchScriptEngine();

    private CountDownLatch scriptStartedLatch = new CountDownLatch(1);
    private CountDownLatch scriptCompletionLatch = new CountDownLatch(1);
    private Logger logger = ESLoggerFactory.getLogger(getClass());

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        return scriptSource;
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        return new ExecutableScript() {
            @Override
            public void setNextVar(String name, Object value) {}

            @Override
            public Object run() {
                scriptStartedLatch.countDown();
                try {
                    if (scriptCompletionLatch.await(10, TimeUnit.SECONDS) == false) {
                        logger.error("Script completion latch was not counted down after 10 seconds");
                    }
                } catch (InterruptedException e) {}
                return true;
            }
        };
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        return null;
    }

    @Override
    public void close() throws IOException {
    }

    public void awaitScriptStartedExecution() throws InterruptedException {
        if (scriptStartedLatch.await(10, TimeUnit.SECONDS) == false) {
            throw new ElasticsearchException("Expected script to be called within 10 seconds, did not happen");
        }
    }

    public void finishScriptExecution() throws InterruptedException {
        scriptCompletionLatch.countDown();
        boolean countedDown = scriptCompletionLatch.await(10, TimeUnit.SECONDS);
        String msg = String.format(Locale.ROOT, "Script completion latch value is [%s], but should be 0", scriptCompletionLatch.getCount());
        assertThat(msg, countedDown, is(true));
    }

    public void reset() {
        scriptStartedLatch = new CountDownLatch(1);
        scriptCompletionLatch = new CountDownLatch(1);
    }

    public static Script latchScript() {
        return new Script(ScriptType.INLINE, NAME, "", Collections.emptyMap());
    }

    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }

    public static class LatchScriptPlugin extends Plugin implements ScriptPlugin {

        @Override
        public ScriptEngine getScriptEngine(Settings settings) {
            return INSTANCE;
        }

        public LatchScriptEngine getScriptEngineService() {
            return INSTANCE;
        }
    }

}
