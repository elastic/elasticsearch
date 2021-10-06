/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;

public class TestTemplateService extends ScriptService {
    private boolean compilationException;

    public static ScriptService instance() {
        return new TestTemplateService(false);
    }

    public static ScriptService instance(boolean compilationException) {
        return new TestTemplateService(compilationException);
    }

    private TestTemplateService(boolean compilationException) {
        super(Settings.EMPTY, Collections.singletonMap(DEFAULT_TEMPLATE_LANG, new MockScriptEngine()), Collections.emptyMap());
        this.compilationException = compilationException;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
        if (this.compilationException) {
            throw new RuntimeException("could not compile script");
        } else {
            return (FactoryType) new MockTemplateScript.Factory(script.getIdOrCode());
        }
    }


    public static class MockTemplateScript extends TemplateScript {
        private final String expected;

        MockTemplateScript(String expected) {
            super(Collections.emptyMap());
            this.expected = expected;
        }

        @Override
        public String execute() {
            return expected;
        }

        public static class Factory implements TemplateScript.Factory {

            private final String expected;

            public Factory(String expected) {
                this.expected = expected;
            }

            @Override
            public TemplateScript newInstance(Map<String, Object> params) {
                return new MockTemplateScript(expected);
            }
        }
    }
}
