/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.fielddata.DoubleScriptFieldData;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.search.runtime.DoubleScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.DoubleScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.DoubleScriptFieldTermQuery;
import org.elasticsearch.search.runtime.DoubleScriptFieldTermsQuery;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public final class DoubleScriptFieldType extends AbstractScriptFieldType<DoubleFieldScript.LeafFactory> {

    private static final DoubleFieldScript.Factory PARSE_FROM_SOURCE
        = (field, params, lookup) -> (DoubleFieldScript.LeafFactory) ctx -> new DoubleFieldScript
        (
            field,
            params,
            lookup,
            ctx
        ) {
        @Override
        public void execute() {
            for (Object v : extractFromSource(field)) {
                if (v instanceof Number) {
                    emit(((Number) v).doubleValue());
                } else if (v instanceof String) {
                    try {
                        emit(Double.parseDouble((String) v));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }
        }
    };

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(name ->
        new Builder<>(name, DoubleFieldScript.CONTEXT, PARSE_FROM_SOURCE) {
            @Override
            RuntimeField newRuntimeField(DoubleFieldScript.Factory scriptFactory) {
                return new DoubleScriptFieldType(name, scriptFactory, getScript(), meta(), this);
            }
        });

    public DoubleScriptFieldType(String name) {
        this(name, PARSE_FROM_SOURCE, null, Collections.emptyMap(), (builder, params) -> builder);
    }

    DoubleScriptFieldType(
        String name,
        DoubleFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        ToXContent toXContent
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
