/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptDoubleFieldData;
import org.elasticsearch.xpack.runtimefields.query.DoubleScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.DoubleScriptFieldRangeQuery;
import org.elasticsearch.xpack.runtimefields.query.DoubleScriptFieldTermQuery;
import org.elasticsearch.xpack.runtimefields.query.DoubleScriptFieldTermsQuery;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ScriptDoubleMappedFieldType extends AbstractScriptMappedFieldType<DoubleFieldScript.LeafFactory> {
    ScriptDoubleMappedFieldType(String name, Script script, DoubleFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, scriptFactory::newFactory, meta);
    }

    @Override
    protected String runtimeType() {
        return NumberType.DOUBLE.typeName();
    }

    @Override
    public Object valueForDisplay(Object value) {
        return value; // These should come back as a Double
    }

    @Override
    public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
        if (timeZone != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
        }
        if (format == null) {
            return DocValueFormat.RAW;
        }
        return new DocValueFormat.Decimal(format);
    }

    @Override
    public ScriptDoubleFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new ScriptDoubleFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new DoubleScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        QueryShardContext context
    ) {
        checkAllowExpensiveQueries(context);
        return NumberType.doubleRangeQuery(
            lowerTerm,
            upperTerm,
            includeLower,
            includeUpper,
            (l, u) -> new DoubleScriptFieldRangeQuery(script, leafFactory(context), name(), l, u)
        );
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new DoubleScriptFieldTermQuery(script, leafFactory(context), name(), NumberType.objectToDouble(value));
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchAllQuery();
        }
        LongSet terms = new LongHashSet(values.size());
        for (Object value : values) {
            terms.add(Double.doubleToLongBits(NumberType.objectToDouble(value)));
        }
        checkAllowExpensiveQueries(context);
        return new DoubleScriptFieldTermsQuery(script, leafFactory(context), name(), terms);
    }
}
