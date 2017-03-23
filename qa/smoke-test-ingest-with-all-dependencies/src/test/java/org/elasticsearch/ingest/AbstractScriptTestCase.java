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
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptMetrics;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static java.util.Collections.emptyList;

public abstract class AbstractScriptTestCase extends ESTestCase {

    protected TemplateService templateService;

    @Before
    public void init() throws Exception {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
            .build();
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(emptyList());
        MustacheScriptEngineService mustache = new MustacheScriptEngineService();
        ScriptSettings scriptSettings = new ScriptSettings(new ScriptEngineRegistry(emptyList()),
                mustache, scriptContextRegistry);

        org.elasticsearch.script.TemplateService esTemplateService =
                new org.elasticsearch.script.TemplateService(settings, new Environment(settings),
                        null, mustache, scriptContextRegistry,
                        scriptSettings, new ScriptMetrics());
        templateService = new InternalTemplateService(esTemplateService);
    }

}
