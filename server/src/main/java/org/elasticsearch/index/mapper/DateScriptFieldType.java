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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.fielddata.DateScriptFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.LongScriptFieldDistanceFeatureQuery;
import org.elasticsearch.search.runtime.LongScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.LongScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.LongScriptFieldTermQuery;
import org.elasticsearch.search.runtime.LongScriptFieldTermsQuery;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class DateScriptFieldType extends AbstractScriptFieldType<DateFieldScript.LeafFactory> {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);

    private static class Builder extends AbstractScriptFieldType.Builder<DateFieldScript.Factory> {
        private final FieldMapper.Parameter<String> format = FieldMapper.Parameter.stringParam(
            "format",
            true,
            initializerNotSupported(),
            null
        ).setSerializer((b, n, v) -> {
            if (v != null && false == v.equals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern())) {
                b.field(n, v);
            }
        }, Object::toString).acceptsNull();

        private final FieldMapper.Parameter<Locale> locale = new FieldMapper.Parameter<>(
            "locale",
            true,
            () -> null,
            (n, c, o) -> o == null ? null : LocaleUtils.parse(o.toString()),
            initializerNotSupported()
        ).setSerializer((b, n, v) -> {
            if (v != null && false == v.equals(Locale.ROOT)) {
                b.field(n, v.toString());
            }
        }, Object::toString).acceptsNull();

        Builder(String name) {
            super(name, DateFieldScript.CONTEXT, DateFieldScript.PARSE_FROM_SOURCE);
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(format);
            parameters.add(locale);
            return Collections.unmodifiableList(parameters);
        }

        @Override
        AbstractScriptFieldType<?> createFieldType(String name, DateFieldScript.Factory factory, Script script, Map<String, String> meta) {
            String pattern = format.getValue() == null ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern() : format.getValue();
            Locale locale = this.locale.getValue() == null ? Locale.ROOT : this.locale.getValue();
            DateFormatter dateTimeFormatter = DateFormatter.forPattern(pattern).withLocale(locale);
            return new DateScriptFieldType(name, factory, dateTimeFormatter, script, meta);
        }
    }

    public static RuntimeField sourceOnly(String name, DateFormatter dateTimeFormatter) {
        Builder builder = new Builder(name);
        builder.format.setValue(dateTimeFormatter.pattern());
        return builder.createRuntimeField(DateFieldScript.PARSE_FROM_SOURCE);
    }

    private final DateFormatter dateTimeFormatter;
    private final DateMathParser dateMathParser;

    DateScriptFieldType(
        String name,
        DateFieldScript.Factory scriptFactory,
        DateFormatter dateTimeFormatter,
        Script script,
        Map<String, String> meta
    ) {
        super(name, searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup, dateTimeFormatter),
            script, meta);
        this.dateTimeFormatter = dateTimeFormatter;
        this.dateMathParser = dateTimeFormatter.toDateMathParser();
    }

    @Override
    public String typeName() {
        return DateFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Object valueForDisplay(Object value) {
        Long val = (Long) value;
        if (val == null) {
            return null;
        }
        return dateTimeFormatter.format(Resolution.MILLISECONDS.toInstant(val).atZone(ZoneOffset.UTC));
    }

    @Override
    public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
        DateFormatter dateTimeFormatter = this.dateTimeFormatter;
        if (format != null) {
            dateTimeFormatter = DateFormatter.forPattern(format).withLocale(dateTimeFormatter.locale());
        }
        if (timeZone == null) {
            timeZone = ZoneOffset.UTC;
        }
        return new DocValueFormat.DateTime(dateTimeFormatter, timeZone, Resolution.MILLISECONDS);
    }

    @Override
    public DateScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> lookup) {
        return new DateScriptFieldData.Builder(name(), leafFactory(lookup.get()));
    }

    @Override
    public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
        checkAllowExpensiveQueries(context);
        return DateFieldType.handleNow(context, now -> {
            long originLong = DateFieldType.parseToLong(
                origin,
                true,
                null,
                this.dateMathParser,
                now,
                DateFieldMapper.Resolution.MILLISECONDS
            );
            TimeValue pivotTime = TimeValue.parseTimeValue(pivot, "distance_feature.pivot");
            return new LongScriptFieldDistanceFeatureQuery(
                script,
                leafFactory(context)::newInstance,
                name(),
                originLong,
                pivotTime.getMillis()
            );
        });
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
        @Nullable DateMathParser parser,
        SearchExecutionContext context
    ) {
        parser = parser == null ? this.dateMathParser : parser;
        checkAllowExpensiveQueries(context);
        return DateFieldType.dateRangeQuery(
            lowerTerm,
            upperTerm,
            includeLower,
            includeUpper,
            timeZone,
            parser,
            context,
            DateFieldMapper.Resolution.MILLISECONDS,
            (l, u) -> new LongScriptFieldRangeQuery(script, leafFactory(context)::newInstance, name(), l, u)
        );
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        return DateFieldType.handleNow(context, now -> {
            long l = DateFieldType.parseToLong(value, false, null, this.dateMathParser, now, DateFieldMapper.Resolution.MILLISECONDS);
            checkAllowExpensiveQueries(context);
            return new LongScriptFieldTermQuery(script, leafFactory(context)::newInstance, name(), l);
        });
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchAllQuery();
        }
        return DateFieldType.handleNow(context, now -> {
            LongSet terms = new LongHashSet(values.size());
            for (Object value : values) {
                terms.add(DateFieldType.parseToLong(value, false, null, this.dateMathParser, now, DateFieldMapper.Resolution.MILLISECONDS));
            }
            checkAllowExpensiveQueries(context);
            return new LongScriptFieldTermsQuery(script, leafFactory(context)::newInstance, name(), terms);
        });
    }
}
