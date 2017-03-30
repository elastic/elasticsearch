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

package org.elasticsearch.script.mustache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptMetrics;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;

/**
 * Validates that {@code contentType} flows properly from {@link TemplateService} into
 * {@link MustacheScriptEngineService} with some basic examples.
 */
public class ContentTypeTests extends ESTestCase {
    private TemplateService templateService;

    @Before
    public void setupTemplateService() throws IOException {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
                .put(ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE, 10000)
                .build();
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(emptyList());
        TemplateService.Backend backend = new MustacheScriptEngineService();
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, backend,
                scriptContextRegistry);
        templateService = new TemplateService(settings, new Environment(settings), null, backend,
                scriptContextRegistry, scriptSettings, new ScriptMetrics());
    }

    public void testNullContentType() {
        testCase("test \"test\" test", null);
    }

    public void testPlainText() {
        testCase("test \"test\" test", CustomMustacheFactory.PLAIN_TEXT_MIME_TYPE);
    }

    public void testJson() {
        testCase("test \\\"test\\\" test", CustomMustacheFactory.JSON_MIME_TYPE);
    }

    public void testJsonWithCharset() {
        testCase("test \\\"test\\\" test", CustomMustacheFactory.JSON_MIME_TYPE_WITH_CHARSET);
    }

    public void testFormEncoded() {
        testCase("test %22test%22 test", CustomMustacheFactory.X_WWW_FORM_URLENCODED_MIME_TYPE);
    }

    private void testCase(String expected, String contentType) {
        assertEquals(expected, templateService
                .template("test {{param}} test", ScriptType.INLINE,
                        ScriptContext.Standard.SEARCH, contentType)
                .apply(singletonMap("param", "\"test\"")).utf8ToString());
        
    }

}
