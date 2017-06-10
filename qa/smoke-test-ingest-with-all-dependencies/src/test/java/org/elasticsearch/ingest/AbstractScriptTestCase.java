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

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;

public abstract class AbstractScriptTestCase extends ESTestCase {

    protected ScriptService scriptService;

    @Before
    public void init() throws Exception {
        MustacheScriptEngine engine = new MustacheScriptEngine();
        Map<String, ScriptEngine> engines = Collections.singletonMap(engine.getType(), engine);
        scriptService = new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    protected TemplateScript.Factory compile(String template) {
        Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, template, Collections.emptyMap());
        return scriptService.compile(script, TemplateScript.CONTEXT);
    }
}
