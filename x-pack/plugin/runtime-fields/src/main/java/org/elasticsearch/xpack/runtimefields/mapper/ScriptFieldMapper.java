/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class ScriptFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "script";

    private static final FieldType FIELD_TYPE = new FieldType();

    ScriptFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, FIELD_TYPE, mappedFieldType, multiFields, copyTo);
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        // there is no field!
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        // TODO implement this
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

    @Override
    public boolean isRuntimeField() {
        return true;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private final ScriptService scriptService;

        private String runtimeType;
        private Script script;

        protected Builder(String name, ScriptService scriptService) {
            super(name, FIELD_TYPE);
            this.scriptService = scriptService;
        }

        public void runtimeType(String runtimeType) {
            this.runtimeType = runtimeType;
        }

        public void script(Script script) {
            this.script = script;
        }

        @Override
        public ScriptFieldMapper build(BuilderContext context) {
            if (runtimeType == null) {
                throw new IllegalArgumentException("runtime_type must be specified");
            }
            if (script == null) {
                throw new IllegalArgumentException("script must be specified");
            }

            MappedFieldType mappedFieldType;
            if (runtimeType.equals("keyword")) {
                StringScriptFieldScript.Factory factory = scriptService.compile(script, StringScriptFieldScript.CONTEXT);
                mappedFieldType = new RuntimeKeywordMappedFieldType(buildFullName(context), script, factory, meta);
            } else {
                throw new IllegalArgumentException("runtime_type [" + runtimeType + "] not supported");
            }
            // TODO copy to and multi_fields... not sure what needs to be done.
            return new ScriptFieldMapper(name, mappedFieldType, multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private final SetOnce<ScriptService> scriptService = new SetOnce<>();

        public void setScriptService(ScriptService scriptService) {
            this.scriptService.set(scriptService);
        }

        @Override
        public ScriptFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            Builder builder = new Builder(name, scriptService.get());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("runtime_type")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [runtime_type] cannot be null.");
                    }
                    builder.runtimeType(XContentMapValues.nodeStringValue(propNode, name + ".runtime_type"));
                    iterator.remove();
                } else if (propName.equals("script")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [script] cannot be null.");
                    }
                    // TODO this should become an object and support the usual script syntax, including lang and params
                    builder.script(new Script(XContentMapValues.nodeStringValue(propNode, name + ".script")));
                    iterator.remove();
                }
            }
            // TODO these get passed in sometimes and we don't need them
            node.remove("doc_values");
            node.remove("index");
            return builder;
        }
    }
}
