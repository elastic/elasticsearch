/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A runtime field of type object. Defines a script at the top level, which emits multiple sub-fields.
 * The sub-fields are declared within the object in order to be made available to the field_caps and search API.
 */
public class CompositeRuntimeField implements RuntimeField {

    public static final String CONTENT_TYPE = "composite";

    public static final Parser PARSER = new Parser(name -> new RuntimeField.Builder(name) {
        private final FieldMapper.Parameter<Script> script = new FieldMapper.Parameter<>(
            "script",
            false,
            () -> null,
            RuntimeField::parseScript,
            RuntimeField.initializerNotSupported(),
            XContentBuilder::field,
            Objects::toString
        ).addValidator(s -> {
            if (s == null) {
                throw new IllegalArgumentException("composite runtime field [" + name + "] must declare a [script]");
            }
        });

        private final FieldMapper.Parameter<OnScriptError> onScriptError = FieldMapper.Parameter.onScriptErrorParam(
            m -> m.onScriptError,
            script
        );

        private final FieldMapper.Parameter<Map<String, Object>> fields = new FieldMapper.Parameter<Map<String, Object>>(
            "fields",
            false,
            Collections::emptyMap,
            (f, p, o) -> parseFields(f, o),
            RuntimeField.initializerNotSupported(),
            XContentBuilder::field,
            Objects::toString
        ).addValidator(objectMap -> {
            if (objectMap == null || objectMap.isEmpty()) {
                throw new IllegalArgumentException("composite runtime field [" + name + "] must declare its [fields]");
            }
        });

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(script);
            parameters.add(fields);
            parameters.add(onScriptError);
            return Collections.unmodifiableList(parameters);
        }

        @Override
        protected RuntimeField createChildRuntimeField(
            MappingParserContext parserContext,
            String parent,
            Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory,
            OnScriptError onScriptError
        ) {
            throw new IllegalArgumentException("Composite field [" + name + "] cannot be a child of composite field [" + parent + "]");
        }

        @Override
        protected RuntimeField createRuntimeField(MappingParserContext parserContext) {
            CompositeFieldScript.Factory factory = parserContext.scriptCompiler().compile(script.get(), CompositeFieldScript.CONTEXT);
            Function<RuntimeField.Builder, RuntimeField> builder = b -> b.createChildRuntimeField(
                parserContext,
                name,
                lookup -> factory.newFactory(name, script.get().getParams(), lookup, onScriptError.get()),
                onScriptError.get()
            );
            Map<String, RuntimeField> runtimeFields = RuntimeField.parseRuntimeFields(
                new HashMap<>(fields.getValue()),
                parserContext,
                builder,
                false
            );
            return new CompositeRuntimeField(name, getParameters(), runtimeFields.values());
        }
    });

    private final String name;
    private final List<FieldMapper.Parameter<?>> parameters;
    private final Collection<RuntimeField> subfields;

    CompositeRuntimeField(String name, List<FieldMapper.Parameter<?>> parameters, Collection<RuntimeField> subfields) {
        this.name = name;
        this.parameters = parameters;
        this.subfields = subfields;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Stream<MappedFieldType> asMappedFieldTypes() {
        return subfields.stream().flatMap(RuntimeField::asMappedFieldTypes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", "composite");
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        for (FieldMapper.Parameter<?> parameter : parameters) {
            parameter.toXContent(builder, includeDefaults);
        }
        builder.endObject();
        return builder;
    }

    private static Map<String, Object> parseFields(String name, Object fieldsObject) {
        if (fieldsObject instanceof Map == false) {
            throw new MapperParsingException(
                "[fields] must be an object, got "
                    + fieldsObject.getClass().getSimpleName()
                    + "["
                    + fieldsObject
                    + "] for field ["
                    + name
                    + "]"
            );
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) fieldsObject;
        return fields;
    }
}
