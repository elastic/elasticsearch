/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class ScriptFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "script";

    private final String runtimeType;
    private final Script script;
    private final Supplier<QueryShardContext> queryShardContextSupplier;

    private static ScriptFieldMapper toType(FieldMapper in) {
        return (ScriptFieldMapper) in;
    }

    protected ScriptFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        String runtimeType,
        Script script,
        Supplier<QueryShardContext> queryShardContextSupplier
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.runtimeType = runtimeType;
        this.script = script;
        this.queryShardContextSupplier = queryShardContextSupplier;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new ScriptFieldMapper.Builder(simpleName(), queryShardContextSupplier).init(this);
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

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> runtimeType = Parameter.stringParam(
            "runtime_type",
            true,
            mapper -> toType(mapper).runtimeType,
            null
        ).setValidator(runtimeType -> {
            if (runtimeType == null) {
                throw new IllegalArgumentException("runtime_type must be specified for script field [" + name + "]");
            }
        });
        // TODO script and runtime_type can be updated: what happens to the currently running queries when they get updated?
        // do all the shards get a consistent view?
        private final Parameter<Script> script = new Parameter<>(
            "script",
            true,
            null,
            Builder::parseScript,
            mapper -> toType(mapper).script
        ).setValidator(script -> {
            if (script == null) {
                throw new IllegalArgumentException("script must be specified for script field [" + name + "]");
            }
        });
        private final Supplier<QueryShardContext> queryShardContextSupplier;

        protected Builder(String name, Supplier<QueryShardContext> queryShardContextSupplier) {
            super(name);
            this.queryShardContextSupplier = queryShardContextSupplier;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, runtimeType, script);
        }

        @Override
        public ScriptFieldMapper build(BuilderContext context) {
            QueryShardContext queryShardContext = queryShardContextSupplier.get();
            MappedFieldType mappedFieldType;
            if (runtimeType.getValue().equals("keyword")) {
                StringScriptFieldScript.Factory factory = queryShardContext.compile(script.getValue(), StringScriptFieldScript.CONTEXT);
                mappedFieldType = new RuntimeKeywordMappedFieldType(buildFullName(context), script.getValue(), factory, meta.getValue());
            } else {
                throw new IllegalArgumentException("runtime_type [" + runtimeType + "] not supported");
            }
            // TODO copy to and multi_fields should not be supported, parametrized field mapper needs to be adapted
            return new ScriptFieldMapper(
                name,
                mappedFieldType,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                runtimeType.getValue(),
                script.getValue(),
                queryShardContextSupplier
            );
        }

        @SuppressWarnings("unchecked")
        static Script parseScript(String name, Mapper.TypeParser.ParserContext parserContext, Object scriptObject) {
            if (scriptObject instanceof Map) {
                Map<String, ?> scriptMap = (Map<String, ?>) scriptObject;
                Object sourceObject = scriptMap.remove("source");
                if (sourceObject == null) {
                    throw new IllegalArgumentException("script source must be specified for script field [" + name + "]");
                }
                Object langObject = scriptMap.remove("lang");
                if (langObject != null && langObject.toString().equals(PainlessScriptEngine.NAME) == false) {
                    throw new IllegalArgumentException(
                        "script lang [" + langObject.toString() + "] not supported for script field [" + name + "]"
                    );
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
                    throw new IllegalArgumentException(
                        "unsupported parameters specified for script field [" + name + "]: " + scriptMap.keySet()
                    );
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

        @Override
        public ScriptFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            ScriptFieldMapper.Builder builder = new ScriptFieldMapper.Builder(name, parserContext.queryShardContextSupplier());
            builder.parse(name, parserContext, node);
            return builder;
        }
    }
}
