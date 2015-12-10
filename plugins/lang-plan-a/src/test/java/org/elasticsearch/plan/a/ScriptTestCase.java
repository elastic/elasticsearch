/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plan.a;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Map;

/**
 * Base test case for scripting unit tests.
 * <p>
 * Typically just asserts the output of {@code exec()}
 */
public abstract class ScriptTestCase extends ESTestCase {
    protected PlanAScriptEngineService scriptEngine;
    
    /** Override to provide different compiler settings */
    protected Settings getSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(PlanAScriptEngineService.NUMERIC_OVERFLOW, random().nextBoolean());
        return builder.build();
    }

    @Before
    public void setup() {
        scriptEngine = new PlanAScriptEngineService(getSettings());
    }

    /** Compiles and returns the result of {@code script} */
    public Object exec(String script) {
        return exec(script, null);
    }

    /** Compiles and returns the result of {@code script} with access to {@code vars} */
    public Object exec(String script, Map<String, Object> vars) {
        Object object = scriptEngine.compile(script);
        CompiledScript compiled = new CompiledScript(ScriptService.ScriptType.INLINE, getTestName(), "plan-a", object);
        return scriptEngine.executable(compiled, vars).run();
    }
}
