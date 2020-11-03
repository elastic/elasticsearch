/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

public final class RuntimeFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "runtime";

    public static final TypeParser PARSER = new TypeParser((name, parserContext) -> new Builder(name, new ScriptCompiler() {
        @Override
        public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
            return parserContext.scriptService().compile(script, context);
        }
    }));

    private final String runtimeType;
    private final Script script;
    private final ScriptCompiler scriptCompiler;

    protected RuntimeFieldMapper(
        String simpleName,
        AbstractScriptFieldType<?> mappedFieldType,
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
    public FieldMapper.Builder getMergeBuilder() {
        return new RuntimeFieldMapper.Builder(simpleName(), scriptCompiler).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        // there is no lucene field
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class Builder extends FieldMapper.Builder {

        static final Map<String, BiFunction<Builder, BuilderContext, AbstractScriptFieldType<?>>> FIELD_TYPE_RESOLVER = Map.of(
            BooleanFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                BooleanFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), BooleanFieldScript.CONTEXT);
                return new BooleanScriptFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            DateFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                DateFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), DateFieldScript.CONTEXT);
                String format = builder.format.getValue();
                if (format == null) {
                    format = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern();
                }
                Locale locale = builder.locale.getValue();
                if (locale == null) {
                    locale = Locale.ROOT;
                }
                DateFormatter dateTimeFormatter = DateFormatter.forPattern(format).withLocale(locale);
                return new DateScriptFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    dateTimeFormatter,
                    builder.meta.getValue()
                );
            },
            NumberType.DOUBLE.typeName(),
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                DoubleFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), DoubleFieldScript.CONTEXT);
                return new DoubleScriptFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            IpFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                IpFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), IpFieldScript.CONTEXT);
                return new IpScriptFieldType(builder.buildFullName(context), builder.script.getValue(), factory, builder.meta.getValue());
            },
            KeywordFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                StringFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), StringFieldScript.CONTEXT);
                return new KeywordScriptFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            GeoPointFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                GeoPointFieldScript.Factory factory = builder.scriptCompiler.compile(
                    builder.script.getValue(),
                    GeoPointFieldScript.CONTEXT
                );
                return new GeoPointScriptFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            NumberType.LONG.typeName(),
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                LongFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), LongFieldScript.CONTEXT);
                return new LongScriptFieldType(builder.buildFullName(context), builder.script.getValue(), factory, builder.meta.getValue());
            }
        );

        private static RuntimeFieldMapper toType(FieldMapper in) {
            return (RuntimeFieldMapper) in;
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
        private final Parameter<String> format = Parameter.stringParam(
            "format",
            true,
            mapper -> ((AbstractScriptFieldType<?>) mapper.fieldType()).format(),
            null
        ).setSerializer((b, n, v) -> {
            if (v != null && false == v.equals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern())) {
                b.field(n, v);
            }
        }, Object::toString).acceptsNull();
        private final Parameter<Locale> locale = new Parameter<>(
            "locale",
            true,
            () -> null,
            (n, c, o) -> o == null ? null : LocaleUtils.parse(o.toString()),
            mapper -> ((AbstractScriptFieldType<?>) mapper.fieldType()).formatLocale()
        ).setSerializer((b, n, v) -> {
            if (v != null && false == v.equals(Locale.ROOT)) {
                b.field(n, v.toString());
            }
        }, Object::toString).acceptsNull();

        private final ScriptCompiler scriptCompiler;

        protected Builder(String name, ScriptCompiler scriptCompiler) {
            super(name);
            this.scriptCompiler = scriptCompiler;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, runtimeType, script, format, locale);
        }

        @Override
        public RuntimeFieldMapper build(BuilderContext context) {
            BiFunction<Builder, BuilderContext, AbstractScriptFieldType<?>> fieldTypeResolver = Builder.FIELD_TYPE_RESOLVER.get(
                runtimeType.getValue()
            );
            if (fieldTypeResolver == null) {
                throw new IllegalArgumentException(
                    "runtime_type [" + runtimeType.getValue() + "] not supported for " + CONTENT_TYPE + " field [" + name + "]"
                );
            }
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [fields]");
            }
            CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [copy_to]");
            }
            return new RuntimeFieldMapper(
                name,
                fieldTypeResolver.apply(this, context),
                MultiFields.empty(),
                CopyTo.empty(),
                runtimeType.getValue(),
                script.getValue(),
                scriptCompiler
            );
        }

        static Script parseScript(String name, Mapper.TypeParser.ParserContext parserContext, Object scriptObject) {
            Script script = Script.parse(scriptObject);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException("stored scripts are not supported for " + CONTENT_TYPE + " field [" + name + "]");
            }
            return script;
        }

        private void formatAndLocaleNotSupported() {
            if (format.getValue() != null) {
                throw new IllegalArgumentException(
                    "format can not be specified for ["
                        + CONTENT_TYPE
                        + "] field ["
                        + name
                        + "] of "
                        + runtimeType.name
                        + " ["
                        + runtimeType.getValue()
                        + "]"
                );
            }
            if (locale.getValue() != null) {
                throw new IllegalArgumentException(
                    "locale can not be specified for ["
                        + CONTENT_TYPE
                        + "] field ["
                        + name
                        + "] of "
                        + runtimeType.name
                        + " ["
                        + runtimeType.getValue()
                        + "]"
                );
            }
        }
    }

    @FunctionalInterface
    private interface ScriptCompiler {
        <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context);
    }
}
