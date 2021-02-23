/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields.mapper;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.runtimefields.fielddata.LongScriptFieldData;
import org.elasticsearch.runtimefields.query.LongScriptFieldExistsQuery;
import org.elasticsearch.runtimefields.query.LongScriptFieldRangeQuery;
import org.elasticsearch.runtimefields.query.LongScriptFieldTermQuery;
import org.elasticsearch.runtimefields.query.LongScriptFieldTermsQuery;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public final class LongScriptFieldType extends AbstractScriptFieldType<LongFieldScript.LeafFactory> {

    public static final RuntimeFieldType.Parser PARSER = new RuntimeFieldType.Parser((name, parserContext) -> new Builder(name) {
        @Override
        protected AbstractScriptFieldType<?> buildFieldType() {
            if (script.get() == null) {
                return new LongScriptFieldType(name, LongFieldScript.PARSE_FROM_SOURCE, this);
            }
            LongFieldScript.Factory factory = parserContext.scriptService().compile(script.getValue(), LongFieldScript.CONTEXT);
            return new LongScriptFieldType(name, factory, this);
        }
    });

    public LongScriptFieldType(String name) {
        this(name, LongFieldScript.PARSE_FROM_SOURCE, null, Collections.emptyMap(), (builder, includeDefaults) -> {});
    }

    private LongScriptFieldType(String name, LongFieldScript.Factory scriptFactory, Builder builder) {
        super(name, scriptFactory::newFactory, builder);
    }

    LongScriptFieldType(
        String name,
        LongFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent
    ) {
        super(name, scriptFactory::newFactory, script, meta, toXContent);
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
        if (timeZone != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
        }
        if (format == null) {
            return DocValueFormat.RAW;
        }
        return new DocValueFormat.Decimal(format);
    }

    @Override
    public LongScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new LongScriptFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        checkAllowExpensiveQueries(context);
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
        checkAllowExpensiveQueries(context);
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
        checkAllowExpensiveQueries(context);
        return new LongScriptFieldTermQuery(script, leafFactory(context)::newInstance, name(), NumberType.objectToLong(value, true));
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchAllQuery();
        }
        LongSet terms = new LongHashSet(values.size());
        for (Object value : values) {
            if (NumberType.hasDecimalPart(value)) {
                continue;
            }
            terms.add(NumberType.objectToLong(value, true));
        }
        if (terms.isEmpty()) {
            return Queries.newMatchNoDocsQuery("All values have a decimal part");
        }
        checkAllowExpensiveQueries(context);
        return new LongScriptFieldTermsQuery(script, leafFactory(context)::newInstance, name(), terms);
    }
}
