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

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.script.mvel.MvelScriptEngineService;

import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptModule extends AbstractModule {

    private List<Class<? extends ScriptEngineService>> scriptEngines = Lists.newArrayList();

    public void addScriptEngine(Class<? extends ScriptEngineService> scriptEngine) {
        scriptEngines.add(scriptEngine);
    }

    @Override protected void configure() {
        Multibinder<ScriptEngineService> multibinder = Multibinder.newSetBinder(binder(), ScriptEngineService.class);
        try {
            multibinder.addBinding().to(MvelScriptEngineService.class);
        } catch (Throwable t) {
            // no MVEL
        }
        for (Class<? extends ScriptEngineService> scriptEngine : scriptEngines) {
            multibinder.addBinding().to(scriptEngine);
        }

        bind(ScriptService.class).asEagerSingleton();
    }
}
