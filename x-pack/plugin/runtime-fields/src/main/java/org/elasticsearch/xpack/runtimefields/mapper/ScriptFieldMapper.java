/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class ScriptFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "script";

    private final String runtimeType;
    private final Script script;

    private static ScriptFieldMapper toType(FieldMapper in) {
        return (ScriptFieldMapper) in;
    }

    protected ScriptFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        // TODO is it ok that the object being built needs to read from the object that is building it? Shouldn't Builder#build return
        // a complete object? Maybe all the parameters need to be passed through instead of the whole builder?
        this.runtimeType = builder.runtimeType.getValue();
        this.script = builder.script.getValue();
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new ScriptFieldMapper.Builder(simpleName(), TypeParser.SCRIPT_SERVICE.get()).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        // there is no lucene field
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        RuntimeKeywordMappedFieldType fieldType = (RuntimeKeywordMappedFieldType) fieldType();
        fieldType.doXContentBody(builder, includeDefaults, params);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = new Parameter<>(
            "meta",
            true,
            Collections.emptyMap(),
            TypeParsers::parseMeta,
            m -> m.fieldType().meta()
        );
        private final Parameter<String> runtimeType = Parameter.stringParam(
            "runtime_type",
            true,
            mapper -> toType(mapper).runtimeType,
            null
        );
        // TODO script and runtime_type can be updated: what happens to the currently running queries when they get updated?
        // do all the shards get a consistent view?
        private final Parameter<Script> script = new Parameter<>("script", true, null,
            Builder::parseScript, mapper -> toType(mapper).script);
        private final ScriptService scriptService;

        protected Builder(String name, ScriptService scriptService) {
            super(name);
            this.scriptService = scriptService;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, runtimeType, script);
        }

        @Override
        public ScriptFieldMapper build(BuilderContext context) {
            if (runtimeType.getValue() == null) {
                throw new IllegalArgumentException("runtime_type must be specified for script field [" + name + "]");
            }
            if (script.getValue() == null) {
                throw new IllegalArgumentException("script must be specified for script field [" + name + "]");
            }

            MappedFieldType mappedFieldType;
            if (runtimeType.getValue().equals("keyword")) {
                StringScriptFieldScript.Factory factory = scriptService.compile(script.getValue(), StringScriptFieldScript.CONTEXT);
                mappedFieldType = new RuntimeKeywordMappedFieldType(buildFullName(context), script.getValue(), factory, meta.getValue());
            } else {
                throw new IllegalArgumentException("runtime_type [" + runtimeType + "] not supported");
            }
            // TODO copy to and multi_fields should not be supported, parametrized field mapper needs to be adapted
            return new ScriptFieldMapper(name, mappedFieldType, multiFieldsBuilder.build(this, context), copyTo.build(), this);
        }

        @SuppressWarnings("unchecked")
        static Script parseScript(String name, Object scriptObject) {
            if (scriptObject instanceof Map) {
                Map<String, ?> scriptMap = (Map<String, ?>) scriptObject;
                Object sourceObject = scriptMap.remove("source");
                if (sourceObject == null) {
                    throw new IllegalArgumentException("script source must be specified for script field [" + name + "]");
                }
                Object langObject = scriptMap.remove("lang");
                if (langObject != null && langObject.toString().equals(PainlessScriptEngine.NAME) == false) {
                    throw new IllegalArgumentException("script lang [" + langObject.toString() + "] not supported for script field ["
                        + name + "]");
                }
                Map<String, Object> params;
                Object paramsObject = scriptMap.remove("params");
                if (paramsObject != null) {
                    if (paramsObject instanceof Map == false) {
                        throw new IllegalArgumentException("unable to parse params for script field [" + name + "]");
                    }
                    params = (Map<String, Object>) paramsObject;
                } else {
                    params = Collections.emptyMap();
                }
                Map<String, String> options;
                Object optionsObject = scriptMap.remove("options");
                if (optionsObject != null) {
                    if (optionsObject instanceof Map == false) {
                        throw new IllegalArgumentException("unable to parse options for script field [" + name + "]");
                    }
                    options = (Map<String, String>) optionsObject;
                } else {
                    options = Collections.emptyMap();
                }
                if (scriptMap.size() > 0) {
                    throw new IllegalArgumentException("unsupported parameters specified for script field [" + name + "]: "
                        + scriptMap.keySet());
                }
                return new Script(ScriptType.INLINE, PainlessScriptEngine.NAME, sourceObject.toString(), options, params);
            } else if (scriptObject instanceof String) {
                return new Script((String) scriptObject);
            } else {
                throw new IllegalArgumentException("unable to parse script for script field [" + name + "]");

            }
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        // TODO this is quite ugly and it's static which makes it even worse
        private static final SetOnce<ScriptService> SCRIPT_SERVICE = new SetOnce<>();

        public void setScriptService(ScriptService scriptService) {
            SCRIPT_SERVICE.set(scriptService);
        }

        @Override
        public ScriptFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            ScriptFieldMapper.Builder builder = new ScriptFieldMapper.Builder(name, SCRIPT_SERVICE.get());
            builder.parse(name, parserContext, node);
            return builder;
        }
    }
}
