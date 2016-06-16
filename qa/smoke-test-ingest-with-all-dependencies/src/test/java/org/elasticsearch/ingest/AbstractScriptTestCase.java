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
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.internal.util.collections.Sets;

import java.util.Arrays;
import java.util.Collections;

public abstract class AbstractScriptTestCase extends ESTestCase {

    protected TemplateService templateService;

    @Before
    public void init() throws Exception {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
            .build();
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Arrays.asList(new MustacheScriptEngineService(settings)));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);

        ScriptService scriptService = new ScriptService(settings, new Environment(settings), null,
                scriptEngineRegistry, scriptContextRegistry, scriptSettings);
        templateService = new InternalTemplateService(scriptService);
    }

}
