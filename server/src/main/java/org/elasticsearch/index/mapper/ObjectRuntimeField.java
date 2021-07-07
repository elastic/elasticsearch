/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ObjectFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A runtime field of type object. Defines a script at the top level, which emits multiple sub-fields.
 * The sub-fields are declared within the object in order to be made available to the field_caps and search API.
 */
public class ObjectRuntimeField implements RuntimeField {

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
            //TODO we call it fields2 for now because it clashes with multi_fields which are rejected for all runtime fields
            private final FieldMapper.Parameter<Map<String, Object>> fields = new FieldMapper.Parameter<>("fields2",
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
            protected RuntimeField createRuntimeField(MappingParserContext parserContext,
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
                return new ObjectRuntimeField(name, runtimeFields.values());
            }
        });

    private final String name;
    private final Collection<RuntimeField> subfields;

    ObjectRuntimeField(String name, Collection<RuntimeField> subfields) {
        this.name = name;
        this.subfields = subfields;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Collection<MappedFieldType> asMappedFieldTypes() {
        return subfields.stream().flatMap(runtimeField -> runtimeField.asMappedFieldTypes().stream()).collect(Collectors.toList());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", "object");
        builder.startObject("fields2");
        for (RuntimeField subfield : subfields) {
            subfield.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
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
}
