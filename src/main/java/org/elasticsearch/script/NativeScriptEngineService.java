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

package org.elasticsearch.script;

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * A native script engine service.
 */
public class NativeScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "native";

    private final ImmutableMap<String, NativeScriptFactory> scripts;

    @Inject
    public NativeScriptEngineService(Settings settings, Map<String, NativeScriptFactory> scripts) {
        super(settings);
        this.scripts = ImmutableMap.copyOf(scripts);
    }

    @Override
    public String[] types() {
        return new String[]{NAME};
    }

    @Override
    public String[] extensions() {
        return new String[0];
    }

    @Override
    public boolean sandboxed() {
        return false;
    }

    @Override
    public Object compile(String script) {
        NativeScriptFactory scriptFactory = scripts.get(script);
        if (scriptFactory != null) {
            return scriptFactory;
        }
        throw new ElasticsearchIllegalArgumentException("Native script [" + script + "] not found");
    }

    @Override
    public ExecutableScript executable(Object compiledScript, @Nullable Map<String, Object> vars) {
        NativeScriptFactory scriptFactory = (NativeScriptFactory) compiledScript;
        return scriptFactory.newScript(vars);
    }

    @Override
    public SearchScript search(Object compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        final NativeScriptFactory scriptFactory = (NativeScriptFactory) compiledScript;
        return new SearchScript() {
            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                AbstractSearchScript script = (AbstractSearchScript) scriptFactory.newScript(vars);
                script.setLookup(lookup.getLeafSearchLookup(context));
                return script;
            }
        };
    }

    @Override
    public Object execute(Object compiledScript, Map<String, Object> vars) {
        return executable(compiledScript, vars).run();
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }

    @Override
    public void close() {
    }

    @Override
    public void scriptRemoved(CompiledScript script) {
        // Nothing to do here
    }
}