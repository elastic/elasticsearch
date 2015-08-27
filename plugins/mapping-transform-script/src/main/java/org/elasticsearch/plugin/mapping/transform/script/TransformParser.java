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

package org.elasticsearch.plugin.mapping.transform.script;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapper.Builder;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.DocumentMapperRootParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.Mapping.SourceTransform;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses transform scripts.
 */
public class TransformParser implements DocumentMapperRootParser {
    private final ScriptService scriptService;

    @Inject
    public TransformParser(ScriptService scriptService) {
        this.scriptService = scriptService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void parse(Builder docBuilder, Object field, Mapper.TypeParser.ParserContext context) {
        if (field instanceof Map) {
            parseTransform(docBuilder, (Map<String, Object>) field, context);
        } else if (field instanceof List) {
            for (Object transformItem : (List<Object>) field) {
                if (!(transformItem instanceof Map)) {
                    throw new MapperParsingException("Elements of transform list must be objects but one was:  " + field);
                }
                parseTransform(docBuilder, (Map<String, Object>) transformItem, context);
            }
        } else {
            throw new MapperParsingException("Transform must be an object or an array but was:  " + field);
        }
    }

    private void parseTransform(DocumentMapper.Builder docBuilder, Map<String, Object> transformConfig,
            Mapper.TypeParser.ParserContext context) {
        Script script = Script.parse(transformConfig, true, context.parseFieldMatcher());
        if (script != null) {
            docBuilder.transform(new ScriptTransform(scriptService, script));
        }
        DocumentMapperParser.checkNoRemainingFields(transformConfig, context.indexVersionCreated(),
                "Transform config has unsupported parameters: ");
    }

    /**
     * Script based source transformation.
     */
    private static class ScriptTransform implements SourceTransform {
        private final ScriptService scriptService;
        /**
         * The script to transform the source document before indexing.
         */
        private final Script script;

        public ScriptTransform(ScriptService scriptService, Script script) {
            this.scriptService = scriptService;
            this.script = script;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map<String, Object> transformSourceAsMap(Map<String, Object> sourceAsMap) {
            try {
                // We use the ctx variable and the _source name to be consistent
                // with the update api.
                ExecutableScript executable = scriptService.executable(script, ScriptContext.Standard.MAPPING);
                Map<String, Object> ctx = new HashMap<>(1);
                ctx.put("_source", sourceAsMap);
                executable.setNextVar("ctx", ctx);
                executable.run();
                ctx = (Map<String, Object>) executable.unwrap(ctx);
                return (Map<String, Object>) ctx.get("_source");
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to execute script", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return script.toXContent(builder, params);
        }
    }
}
