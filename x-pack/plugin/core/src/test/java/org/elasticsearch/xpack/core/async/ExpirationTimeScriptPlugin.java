/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
