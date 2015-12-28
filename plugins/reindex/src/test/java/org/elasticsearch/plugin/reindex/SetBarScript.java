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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptModule;

import java.util.Map;

/**
 * Script used to test update-by-query style use cases.
 */
public class SetBarScript extends AbstractExecutableScript {
    private final Object newValue;
    private Map<String, Object> ctx;

    public SetBarScript(Object newValue) {
        this.newValue = newValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setNextVar(String name, Object value) {
        if (name.equals("ctx")) {
            ctx = (Map<String, Object>) value;
            return;
        }
        throw new IllegalArgumentException("Unexpected variable [" + name + "]");
    }

    @Override
    public Object run() {
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
        source.put("bar", newValue);
        return null;
    }

    public static class RegistrationPlugin extends Plugin {
        @Override
        public String name() {
            return "set-bar-script";
        }

        @Override
        public String description() {
            return "test plugin";
        }

        public void onModule(ScriptModule scripts) {
            scripts.registerScript("set-bar", Factory.class);
        }
    }

    public static class Factory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(Map<String, Object> params) {
            return new SetBarScript(params.get("to"));
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
