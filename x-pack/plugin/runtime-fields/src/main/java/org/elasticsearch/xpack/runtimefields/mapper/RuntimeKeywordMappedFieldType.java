/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptBinaryFieldData;

import java.io.IOException;
import java.util.Map;

public final class RuntimeKeywordMappedFieldType extends MappedFieldType {

    private final Script script;
    private final StringScriptFieldScript.Factory scriptFactory;

    RuntimeKeywordMappedFieldType(String name, Script script, StringScriptFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
        this.script = script;
        this.scriptFactory = scriptFactory;
    }

    RuntimeKeywordMappedFieldType(RuntimeKeywordMappedFieldType ref) {
        super(ref);
        this.script = ref.script;
        this.scriptFactory = ref.scriptFactory;
    }

    @Override
    public MappedFieldType clone() {
        return new RuntimeKeywordMappedFieldType(this);
    }

    @Override
    public Object valueForDisplay(Object value) {
        if (value == null) {
            return null;
        }
        // keywords are internally stored as utf8 bytes
        BytesRef binaryValue = (BytesRef) value;
        return binaryValue.utf8ToString();
    }

    @Override
    public String typeName() {
        // TODO not sure what we should return here: the runtime type or the field type?
        // why is the same string returned from three different methods?
        return ScriptFieldMapper.CONTENT_TYPE;
    }


    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        // TODO once we get SearchLookup as an argument, we can already call scriptFactory.newFactory here and pass through the result
        return new ScriptBinaryFieldData.Builder(scriptFactory);
    }

    private String toValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return value.toString();
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        // TODO cache the runtimeValues in the context somehow
        LogManager.getLogger().error("ASDFDAF [{}]", toValue(value));
        return scriptFactory.newFactory(script.getParams(), context.lookup()).runtimeValues().termQuery(name(), toValue(value));
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        return null;
    }

    void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("runtime_type", "keyword");
        builder.field("script", script.getIdOrCode()); // TODO For some reason this doesn't allow us to do the full xcontent of the script.
    }

    // TODO do we need to override equals/hashcode?
}
