/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.script;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.MapMaker;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.mvel.MvelScriptEngineService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptService extends AbstractComponent {

    private final String defaultType;

    private final ImmutableMap<String, ScriptEngineService> scriptEngines;

    private final ConcurrentMap<String, CompiledScript> cache = new MapMaker().softValues().makeMap();

    public ScriptService(Settings settings) {
        this(settings, ImmutableSet.<ScriptEngineService>builder()
                .add(new MvelScriptEngineService(settings))
                .build()
        );
    }

    @Inject public ScriptService(Settings settings, Set<ScriptEngineService> scriptEngines) {
        super(settings);

        this.defaultType = componentSettings.get("default_type", "mvel");

        ImmutableMap.Builder<String, ScriptEngineService> builder = ImmutableMap.builder();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            builder.put(scriptEngine.type(), scriptEngine);
        }
        this.scriptEngines = builder.build();
    }

    public CompiledScript compile(String script) {
        return compile(defaultType, script);
    }

    public CompiledScript compile(String type, String script) {
        CompiledScript compiled = cache.get(script);
        if (compiled != null) {
            return compiled;
        }
        if (type == null) {
            type = defaultType;
        }
        synchronized (cache) {
            compiled = cache.get(script);
            if (compiled != null) {
                return compiled;
            }
            ScriptEngineService service = scriptEngines.get(type);
            if (service == null) {
                throw new ElasticSearchIllegalArgumentException("script_type not supported [" + type + "]");
            }
            compiled = new CompiledScript(type, service.compile(script));
            cache.put(script, compiled);
        }
        return compiled;
    }

    public Object execute(CompiledScript compiledScript, Map vars) {
        return scriptEngines.get(compiledScript.type()).execute(compiledScript.compiled(), vars);
    }

    public void clear() {
        cache.clear();
    }

}
