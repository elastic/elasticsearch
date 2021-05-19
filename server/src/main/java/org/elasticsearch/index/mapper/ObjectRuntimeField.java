/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ObjectFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A runtime field of type object. Defines a script at the top level, which emits multiple sub-fields.
 * The sub-fields are declared within the object in order to be made available to the field_caps and search API.
 */
//TODO find a better name!?
public class ObjectRuntimeField implements RuntimeField, DynamicFieldType {

    public static final Parser PARSER = new Parser(name ->
        new RuntimeField.Builder(name) {
            private final FieldMapper.Parameter<Script> script = new FieldMapper.Parameter<>(
                "script",
                true,
                () -> null,
                AbstractScriptFieldType.Builder::parseScript,
                null
            ).setSerializerCheck((id, ic, v) -> ic).setValidator(s -> {
                if (s == null) {
                    throw new IllegalArgumentException("object runtime field [" + name + "] must declare a [script]");
                }
            });
            private final FieldMapper.Parameter<Map<String, Object>> fields = new FieldMapper.Parameter<>("fields",
                true, Collections::emptyMap, (f, p, o) -> parseFields(f, o), mappers -> {
                throw new UnsupportedOperationException();
            });

            @Override
            protected List<FieldMapper.Parameter<?>> getParameters() {
                List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
                parameters.add(script);
                parameters.add(fields);
                return Collections.unmodifiableList(parameters);
            }

            @Override
            protected RuntimeField createRuntimeField(Mapper.TypeParser.ParserContext parserContext,
                                                      Function<SearchLookup, ObjectFieldScript.LeafFactory> parentScriptFactory) {
                if (parentScriptFactory != null) {
                    throw new IllegalArgumentException("Runtime field [" + name + "] of type [object] cannot hold another runtime " +
                        "field of type [object]");
                }
                ObjectFieldScript.Factory factory = parserContext.scriptCompiler().compile(script.get(), ObjectFieldScript.CONTEXT);
                //TODO the sub-fields should be named after their full path, yet their toXContent should
                // only print out their leaf field name (which may contain dots!)
                Map<String, RuntimeField> runtimeFields = RuntimeField.parseRuntimeFields(fields.getValue(),
                    parserContext, searchLookup -> factory.newFactory(name, script.get().getParams(), searchLookup), false);
                return new ObjectRuntimeField(name, runtimeFields, this);
            }
        });

    private final String name;
    private final Map<String, RuntimeField> subfields;
    private final ToXContent toXContent;

    ObjectRuntimeField(String name, Map<String, RuntimeField> subfields, ToXContent toXContent) {
        this.name = name;
        this.subfields = subfields;
        this.toXContent = toXContent;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        toXContent.toXContent(builder, params);
        builder.startObject("fields");
        for (RuntimeField runtimeField : subfields.values()) {
            runtimeField.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String typeName() {
        return "object";
    }

    @Override
    public MappedFieldType asMappedFieldType() {
        return null;
    }

    private static Map<String, Object> parseFields(String name, Object fieldsObject) {
        if (fieldsObject instanceof Map == false) {
            throw new MapperParsingException("[fields] must be an object, got " + fieldsObject.getClass().getSimpleName() +
                "[" + fieldsObject + "] for field [" + name +"]");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) fieldsObject;
        return fields;
    }

    @Override
    public MappedFieldType getChildFieldType(String path) {
        RuntimeField runtimeField = subfields.get(path);
        //we could create a KeywordScriptFieldType on-the-fly for any unmapped field, but they would not be exposed to field_caps
        return runtimeField == null ? null : runtimeField.asMappedFieldType();
    }

    @Override
    public Set<String> getKnownSubfields() {
        return subfields.keySet();
    }
}
