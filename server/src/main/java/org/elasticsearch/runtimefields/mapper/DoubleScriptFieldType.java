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
import org.elasticsearch.runtimefields.fielddata.DoubleScriptFieldData;
import org.elasticsearch.runtimefields.query.DoubleScriptFieldExistsQuery;
import org.elasticsearch.runtimefields.query.DoubleScriptFieldRangeQuery;
import org.elasticsearch.runtimefields.query.DoubleScriptFieldTermQuery;
import org.elasticsearch.runtimefields.query.DoubleScriptFieldTermsQuery;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public final class DoubleScriptFieldType extends AbstractScriptFieldType<DoubleFieldScript.LeafFactory> {

    public static final RuntimeFieldType.Parser PARSER = new RuntimeFieldTypeParser((name, parserContext) -> new Builder(name) {
        @Override
        protected AbstractScriptFieldType<?> buildFieldType() {
            if (script.get() == null) {
                return new DoubleScriptFieldType(name, DoubleFieldScript.PARSE_FROM_SOURCE, this);
            }
            DoubleFieldScript.Factory factory = parserContext.scriptService().compile(script.getValue(), DoubleFieldScript.CONTEXT);
            return new DoubleScriptFieldType(name, factory, this);
        }
    });

    private DoubleScriptFieldType(String name, DoubleFieldScript.Factory scriptFactory, Builder builder) {
        super(name, scriptFactory::newFactory, builder);
    }

    DoubleScriptFieldType(String name) {
        this(name, DoubleFieldScript.PARSE_FROM_SOURCE, null, Collections.emptyMap(), (builder, includeDefaults) -> {});
    }

    DoubleScriptFieldType(
        String name,
        DoubleFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent
    ) {
        super(name, scriptFactory::newFactory, script, meta, toXContent);
    }

    @Override
    public String typeName() {
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
    public DoubleScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new DoubleScriptFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
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
        SearchExecutionContext context
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
    public Query termQuery(Object value, SearchExecutionContext context) {
        checkAllowExpensiveQueries(context);
        return new DoubleScriptFieldTermQuery(script, leafFactory(context), name(), NumberType.objectToDouble(value));
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
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
