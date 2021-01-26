/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

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

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.script.MockScriptPlugin;
import org.junit.Assert;

import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.contains;

public class ExpirationTimeScriptPlugin extends MockScriptPlugin {
    @Override
    public String pluginScriptLang() {
        return "painless";
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
        final String fieldName = "expiration_time";
        final String script =
            " if (ctx._source.expiration_time < params.expiration_time) { " +
            "     ctx._source.expiration_time = params.expiration_time; " +
            " } else { " +
            "     ctx.op = \"noop\"; " +
            " }";
        return Map.of(
            script, vars -> {
                Map<String, Object> params = (Map<String, Object>) vars.get("params");
                Assert.assertNotNull(params);
                Assert.assertThat(params.keySet(), contains(fieldName));
                long updatingValue = (long) params.get(fieldName);

                Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                Assert.assertNotNull(ctx);
                Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                long currentValue = (long) source.get(fieldName);
                if (currentValue < updatingValue) {
                    source.put(fieldName, updatingValue);
                } else {
                    ctx.put("op", "noop");
                }
                return ctx;
            }
        );
    }
}
