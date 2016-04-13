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

package org.elasticsearch.index.reindex;

import org.elasticsearch.script.ExecutableScript;

import java.util.Map;
import java.util.function.Consumer;

public class SimpleExecutableScript implements ExecutableScript {
    private final Consumer<Map<String, Object>> script;
    private Map<String, Object> ctx;

    public SimpleExecutableScript(Consumer<Map<String, Object>> script) {
        this.script = script;
    }

    @Override
    public Object run() {
        script.accept(ctx);
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setNextVar(String name, Object value) {
        if ("ctx".equals(name)) {
            ctx = (Map<String, Object>) value;
        } else {
            throw new IllegalArgumentException("Unsupported var [" + name + "]");
        }
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }
}
