/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptLongFieldData;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldTermQuery;

import java.util.Map;

public class ScriptLongMappedFieldType extends AbstractScriptMappedFieldType {
    private final LongScriptFieldScript.Factory scriptFactory;

    ScriptLongMappedFieldType(String name, Script script, LongScriptFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, meta);
        this.scriptFactory = scriptFactory;
    }

    @Override
    protected String runtimeType() {
        return NumberType.LONG.typeName();
    }

    @Override
    public Object valueForDisplay(Object value) {
        return value; // These should come back as a Long
    }

    @Override
    public ScriptLongFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        // TODO once we get SearchLookup as an argument, we can already call scriptFactory.newFactory here and pass through the result
        return new ScriptLongFieldData.Builder(script, scriptFactory);
    }

    private LongScriptFieldScript.LeafFactory leafFactory(QueryShardContext context) {
        return scriptFactory.newFactory(script.getParams(), context.lookup());
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new LongScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        if (NumberType.hasDecimalPart(value)) {
            return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
        }
        checkAllowExpensiveQueries(context);
        return new LongScriptFieldTermQuery(script, leafFactory(context), name(), NumberType.objectToLong(value, true));
    }
}
