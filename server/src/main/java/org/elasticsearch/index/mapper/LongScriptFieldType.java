/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.LongScriptFieldData;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.LongScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.LongScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.LongScriptFieldTermQuery;
import org.elasticsearch.search.runtime.LongScriptFieldTermsQuery;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public final class LongScriptFieldType extends AbstractScriptFieldType<LongFieldScript.LeafFactory> {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);

    private static class Builder extends AbstractScriptFieldType.Builder<LongFieldScript.Factory> {
        Builder(String name) {
            super(name, LongFieldScript.CONTEXT);
        }

        @Override
        AbstractScriptFieldType<?> createFieldType(String name, LongFieldScript.Factory factory, Script script, Map<String, String> meta) {
            return new LongScriptFieldType(name, factory, script, meta);
        }

        @Override
        LongFieldScript.Factory getParseFromSourceFactory() {
            return LongFieldScript.PARSE_FROM_SOURCE;
        }

        @Override
        LongFieldScript.Factory getCompositeLeafFactory(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory) {
            return LongFieldScript.leafAdapter(parentScriptFactory);
        }
    }

    public static RuntimeField sourceOnly(String name) {
        return new Builder(name).createRuntimeField(LongFieldScript.PARSE_FROM_SOURCE);
    }

    public LongScriptFieldType(String name, LongFieldScript.Factory scriptFactory, Script script, Map<String, String> meta) {
        super(
            name,
            searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup),
            script,
            scriptFactory.isResultDeterministic(),
            meta
        );
    }

    @Override
    public String typeName() {
        return NumberType.LONG.typeName();
    }

    @Override
    public Object valueForDisplay(Object value) {
        return value; // These should come back as a Long
    }

    @Override
    public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
        checkNoTimeZone(timeZone);
        if (format == null) {
            return DocValueFormat.RAW;
        }
        return new DocValueFormat.Decimal(format);
    }

    @Override
    public LongScriptFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        return new LongScriptFieldData.Builder(name(), leafFactory(fieldDataContext.lookupSupplier().get()), LongDocValuesField::new);
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        applyScriptContext(context);
        return new LongScriptFieldExistsQuery(script, leafFactory(context)::newInstance, name());
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
    ) {
        applyScriptContext(context);
        return NumberType.longRangeQuery(
            lowerTerm,
            upperTerm,
            includeLower,
            includeUpper,
            (l, u) -> new LongScriptFieldRangeQuery(script, leafFactory(context)::newInstance, name(), l, u)
        );
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        if (NumberType.hasDecimalPart(value)) {
            return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
        }
        applyScriptContext(context);
        return new LongScriptFieldTermQuery(script, leafFactory(context)::newInstance, name(), NumberType.objectToLong(value, true));
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchAllQuery();
        }
        Set<Long> terms = Sets.newHashSetWithExpectedSize(values.size());
        for (Object value : values) {
            if (NumberType.hasDecimalPart(value)) {
                continue;
            }
            terms.add(NumberType.objectToLong(value, true));
        }
        if (terms.isEmpty()) {
            return Queries.newMatchNoDocsQuery("All values have a decimal part");
        }
        applyScriptContext(context);
        return new LongScriptFieldTermsQuery(script, leafFactory(context)::newInstance, name(), terms);
    }
}
