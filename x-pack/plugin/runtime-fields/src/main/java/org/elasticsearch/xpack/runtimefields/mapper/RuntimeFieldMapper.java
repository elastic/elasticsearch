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
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

public final class RuntimeFieldMapper extends ParametrizedFieldMapper {

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
        AbstractScriptMappedFieldType<?> mappedFieldType,
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
        return new RuntimeFieldMapper.Builder(simpleName(), scriptCompiler).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        // there is no lucene field
    }

    @Override
    public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup lookup, String format) {
        return new DocValueFetcher(fieldType().docValueFormat(format, null), lookup.doc().getForField(fieldType()));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        static final Map<String, BiFunction<Builder, BuilderContext, AbstractScriptMappedFieldType<?>>> FIELD_TYPE_RESOLVER = Map.of(
            BooleanFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                String name = builder.buildFullName(context);
                Script script = builder.script.getValue();
                BooleanFieldScript.Factory factory = builder.scriptCompiler.compile(script, BooleanFieldScript.CONTEXT);
                return new BooleanScriptMappedFieldType(name, script, builder.meta.getValue()) {
                    @Override
                    protected BooleanFieldScript.LeafFactory leafFactory(SearchLookup searchLookup) {
                        return factory.newFactory(name, script.getParams(), searchLookup);
                    }
                };
            },
            DateFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                String name = builder.buildFullName(context);
                Script script = builder.script.getValue();
                String format = builder.format.getValue();
                if (format == null) {
                    format = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern();
                }
                Locale locale = builder.locale.getValue();
                if (locale == null) {
                    locale = Locale.ROOT;
                }
                DateFormatter dateTimeFormatter = DateFormatter.forPattern(format).withLocale(locale);
                DateFieldScript.Factory factory = builder.scriptCompiler.compile(script, DateFieldScript.CONTEXT);
                return new DateScriptMappedFieldType(name, script, dateTimeFormatter, builder.meta.getValue()) {
                    @Override
                    protected DateFieldScript.LeafFactory leafFactory(SearchLookup searchLookup) {
                        return factory.newFactory(name, script.getParams(), searchLookup, dateTimeFormatter);
                    }
                };
            },
            NumberType.DOUBLE.typeName(),
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                String name = builder.buildFullName(context);
                Script script = builder.script.getValue();
                DoubleFieldScript.Factory factory = builder.scriptCompiler.compile(script, DoubleFieldScript.CONTEXT);
                return new DoubleScriptMappedFieldType(name, script, builder.meta.getValue()) {
                    @Override
                    protected DoubleFieldScript.LeafFactory leafFactory(SearchLookup searchLookup) {
                        return factory.newFactory(name, script.getParams(), searchLookup);
                    }
                };
            },
            IpFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                String name = builder.buildFullName(context);
                Script script = builder.script.getValue();
                IpFieldScript.Factory factory = builder.scriptCompiler.compile(script, IpFieldScript.CONTEXT);
                return new IpScriptMappedFieldType(name, script, builder.meta.getValue()) {
                    @Override
                    protected IpFieldScript.LeafFactory leafFactory(SearchLookup searchLookup) {
                        return factory.newFactory(name, script.getParams(), searchLookup);
                    }
                };
            },
            KeywordFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                String name = builder.buildFullName(context);
                Script script = builder.script.getValue();
                StringFieldScript.Factory factory = builder.scriptCompiler.compile(script, StringFieldScript.CONTEXT);
                return new KeywordScriptMappedFieldType(name, script, builder.meta.getValue()) {
                    @Override
                    protected StringFieldScript.LeafFactory leafFactory(SearchLookup searchLookup) {
                        return factory.newFactory(name, script.getParams(), searchLookup);
                    }
                };
            },
            NumberType.LONG.typeName(),
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                String name = builder.buildFullName(context);
                Script script = builder.script.getValue();
                LongFieldScript.Factory factory = builder.scriptCompiler.compile(script, LongFieldScript.CONTEXT);
                return new LongScriptMappedFieldType(name, script, builder.meta.getValue()) {
                    @Override
                    protected LongFieldScript.LeafFactory leafFactory(SearchLookup searchLookup) {
                        return factory.newFactory(name, script.getParams(), searchLookup);
                    }
                };
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
            mapper -> ((AbstractScriptMappedFieldType<?>) mapper.fieldType()).format(),
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
            mapper -> ((AbstractScriptMappedFieldType<?>) mapper.fieldType()).formatLocale()
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
            BiFunction<Builder, BuilderContext, AbstractScriptMappedFieldType<?>> fieldTypeResolver = Builder.FIELD_TYPE_RESOLVER.get(
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
