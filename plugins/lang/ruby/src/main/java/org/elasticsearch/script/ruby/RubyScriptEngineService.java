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

package org.elasticsearch.script.ruby;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.jruby.embed.EmbedEvalUnit;
import org.jruby.embed.ScriptingContainer;

import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
// Need to figure out how to execute compiled scripts in the most optimized manner (passing paramters to them).
public class RubyScriptEngineService extends AbstractComponent implements ScriptEngineService {

    private final ScriptingContainer container;

    @Inject public RubyScriptEngineService(Settings settings) {
        super(settings);

        this.container = new ScriptingContainer();
        container.setClassLoader(settings.getClassLoader());
    }

    @Override public String[] types() {
        return new String[]{"ruby"};
    }

    @Override public Object compile(String script) {
        return container.parse(script);
    }

    @Override public ExecutableScript executable(Object compiledScript, Map<String, Object> vars) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override public Object execute(Object compiledScript, Map<String, Object> vars) {
        EmbedEvalUnit unit = (EmbedEvalUnit) compiledScript;
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override public void close() {
        container.clear();
    }
}
