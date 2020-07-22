/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public final class RuntimeScriptFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "runtime_script";

    private final String runtimeType;
    private final Script script;
    private final ScriptCompiler scriptCompiler;

    protected RuntimeScriptFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        String runtimeType,
        Script script,
        ScriptCompiler scriptCompiler
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.runtimeType = runtimeType;
        this.script = script;
        this.scriptCompiler = scriptCompiler;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new RuntimeScriptFieldMapper.Builder(simpleName(), scriptCompiler).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        // there is no lucene field
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        static final Map<String, BiFunction<Builder, BuilderContext, MappedFieldType>> FIELD_TYPE_RESOLVER = Map.of(
            NumberType.DOUBLE.typeName(),
            (builder, context) -> {
                DoubleScriptFieldScript.Factory factory = builder.scriptCompiler.compile(
                    builder.script.getValue(),
                    DoubleScriptFieldScript.CONTEXT
                );
                return new ScriptDoubleMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            KeywordFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                StringScriptFieldScript.Factory factory = builder.scriptCompiler.compile(
                    builder.script.getValue(),
                    StringScriptFieldScript.CONTEXT
                );
                return new ScriptKeywordMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            NumberType.LONG.typeName(),
            (builder, context) -> {
                LongScriptFieldScript.Factory factory = builder.scriptCompiler.compile(
                    builder.script.getValue(),
                    LongScriptFieldScript.CONTEXT
                );
                return new ScriptLongMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            }
        );

        private static RuntimeScriptFieldMapper toType(FieldMapper in) {
            return (RuntimeScriptFieldMapper) in;
        }

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> runtimeType = Parameter.stringParam(
            "runtime_type",
            true,
            mapper -> toType(mapper).runtimeType,
            null
        ).setValidator(runtimeType -> {
            if (runtimeType == null) {
                throw new IllegalArgumentException("runtime_type must be specified for " + CONTENT_TYPE + " field [" + name + "]");
            }
        });
        private final Parameter<Script> script = new Parameter<>(
            "script",
            true,
            () -> null,
            Builder::parseScript,
            mapper -> toType(mapper).script
        ).setValidator(script -> {
            if (script == null) {
                throw new IllegalArgumentException("script must be specified for " + CONTENT_TYPE + " field [" + name + "]");
            }
        });

        private final ScriptCompiler scriptCompiler;

        protected Builder(String name, ScriptCompiler scriptCompiler) {
            super(name);
            this.scriptCompiler = scriptCompiler;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, runtimeType, script);
        }

        @Override
        public RuntimeScriptFieldMapper build(BuilderContext context) {
            BiFunction<Builder, BuilderContext, MappedFieldType> fieldTypeResolver = Builder.FIELD_TYPE_RESOLVER.get(
                runtimeType.getValue()
            );
            if (fieldTypeResolver == null) {
                throw new IllegalArgumentException(
                    "runtime_type [" + runtimeType.getValue() + "] not supported for " + CONTENT_TYPE + " field [" + name + "]"
                );
            }
            // TODO copy to and multi_fields should not be supported, parametrized field mapper needs to be adapted
            return new RuntimeScriptFieldMapper(
                name,
                fieldTypeResolver.apply(this, context),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                runtimeType.getValue(),
                script.getValue(),
                scriptCompiler
            );
        }

        static Script parseScript(String name, Mapper.TypeParser.ParserContext parserContext, Object scriptObject) {
            Script script = Script.parse(scriptObject);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException(
                    "stored scripts specified but not supported for " + CONTENT_TYPE + " field [" + name + "]"
                );
            }
            return script;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public RuntimeScriptFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {

            RuntimeScriptFieldMapper.Builder builder = new RuntimeScriptFieldMapper.Builder(name, new ScriptCompiler() {
                @Override
                public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                    return parserContext.queryShardContextSupplier().get().compile(script, context);
                }
            });
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

    @FunctionalInterface
    private interface ScriptCompiler {
        <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context);
    }
}
