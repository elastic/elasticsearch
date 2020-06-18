/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptBinaryFieldData;

import java.util.Map;

public final class RuntimeKeywordMappedFieldType extends MappedFieldType {

    private final StringScriptFieldScript.Factory scriptFactory;

    RuntimeKeywordMappedFieldType(String name, StringScriptFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, false, false, meta);
        this.scriptFactory = scriptFactory;
    }

    RuntimeKeywordMappedFieldType(RuntimeKeywordMappedFieldType ref) {
        super(ref);
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
        //TODO not sure what we should return here: the runtime type or the field type?
        // why is the same string returned from three methods?
        return ScriptFieldMapper.CONTENT_TYPE;
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        //TODO need to find an ok way to get SourceLookup from the QueryShardContext, as well as the rest of the stuff
        StringScriptFieldScript.LeafFactory leafFactory = scriptFactory.newFactory(null, null, null);
        return new ScriptBinaryFieldData.Builder(leafFactory);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return null;
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        return null;
    }

    //TODO do we need to override equals/hashcode?
}
