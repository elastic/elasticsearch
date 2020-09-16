/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xpack.runtimefields.mapper.RuntimeFieldMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class RuntimeFields extends Plugin implements MapperPlugin, ScriptPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(RuntimeFieldMapper.CONTENT_TYPE, RuntimeFieldMapper.PARSER);
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return List.of(
            BooleanScriptFieldScript.CONTEXT,
            DateScriptFieldScript.CONTEXT,
            DoubleScriptFieldScript.CONTEXT,
            IpScriptFieldScript.CONTEXT,
            LongScriptFieldScript.CONTEXT,
            StringScriptFieldScript.CONTEXT
        );
    }
}
