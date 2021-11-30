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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.fielddata.LongScriptFieldData;
import org.elasticsearch.index.mapper.FieldMapper.Parameter;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.queryableexpression.QueryableExpression;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.LongFieldScript.LeafFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.LongScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.LongScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.LongScriptFieldTermQuery;
import org.elasticsearch.search.runtime.LongScriptFieldTermsQuery;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class LongScriptFieldType extends AbstractScriptFieldType<LongFieldScript.LeafFactory> {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);

    private static class Builder extends AbstractScriptFieldType.Builder<LongFieldScript.Factory> {
        private final Parameter<Boolean> approximateFirst = Parameter.boolParam(
            "approximate_first",
            false,
            RuntimeField.initializerNotSupported(),
            false
        );

        Builder(String name) {
            super(name, LongFieldScript.CONTEXT);
        }

        @Override
        AbstractScriptFieldType<?> createFieldType(String name, LongFieldScript.Factory factory, Script script, Map<String, String> meta) {
            return new LongScriptFieldType(name, factory, script, approximateFirst.get(), meta);
        }

        @Override
        LongFieldScript.Factory getParseFromSourceFactory() {
            return LongFieldScript.PARSE_FROM_SOURCE;
        }

        @Override
        LongFieldScript.Factory getCompositeLeafFactory(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory) {
            return LongFieldScript.leafAdapter(parentScriptFactory);
        }

        @Override
        protected LongFieldScript.Factory compile(
            MappingParserContext parserContext,
            Script script,
            ScriptContext<LongFieldScript.Factory> scriptContext
        ) {
            if (approximateFirst.get()) {
                // Enable argument collection in painless so it'll implement emitExpression
                Map<String, String> options = new HashMap<>(script.getOptions());
                options.put("collect_arguments", "true");
                script = new Script(script.getType(), script.getLang(), script.getIdOrCode(), options, script.getParams());
            }
            return super.compile(parserContext, script, scriptContext);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            List<Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(approximateFirst);
            return parameters;
        }
    }

    public static RuntimeField sourceOnly(String name) {
        return new Builder(name).createRuntimeField(LongFieldScript.PARSE_FROM_SOURCE);
    }

    private final boolean approximateFirst;

    public LongScriptFieldType(
        String name,
        LongFieldScript.Factory scriptFactory,
        Script script,
        boolean approximateFirst,
        Map<String, String> meta
    ) {
        super(name, new Factory<LongFieldScript.LeafFactory>() {
            @Override
            public LeafFactory leafFactory(SearchLookup searchLookup) {
                return scriptFactory.newFactory(name, script.getParams(), searchLookup);
            }

            @Override
            public QueryableExpression queryableExpression(Function<String, QueryableExpression> lookup) {
                return scriptFactory.emitExpression().build(lookup, script.getParams()::get);
            }
        }, script, scriptFactory.isResultDeterministic(), meta);
        this.approximateFirst = approximateFirst;
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
    public LongScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new LongScriptFieldData.Builder(name(), leafFactory(searchLookup.get()), LongDocValuesField::new);
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
        return NumberType.longRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
            if (l > u) {
                return new MatchNoDocsQuery(l + " > " + u);
            }
            Query approximation = approximateFirst
                ? queryableExpression(context).castToLong().approximateRangeQuery(l, u)
                : new MatchAllDocsQuery();
            return new LongScriptFieldRangeQuery(script, name(), approximation, leafFactory(context)::newInstance, l, u);
        });
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        if (NumberType.hasDecimalPart(value)) {
            return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
        }
        applyScriptContext(context);
        long v = NumberType.objectToLong(value, true);
        Query approximation = approximateFirst
            ? queryableExpression(context).castToLong().approximateTermQuery(v)
            : new MatchAllDocsQuery();
        return new LongScriptFieldTermQuery(script, name(), approximation, leafFactory(context)::newInstance, v);
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
        applyScriptContext(context);
        return new LongScriptFieldTermsQuery(script, leafFactory(context)::newInstance, name(), terms);
    }
}
