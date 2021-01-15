/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.fielddata.DateScriptFieldData;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldDistanceFeatureQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldRangeQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldTermQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldTermsQuery;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class DateScriptFieldType extends AbstractScriptFieldType<DateFieldScript.LeafFactory> {

    public static final RuntimeFieldType.Parser PARSER = new RuntimeFieldTypeParser((name, parserContext) -> new Builder(name) {
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

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(format);
            parameters.add(locale);
            return Collections.unmodifiableList(parameters);
        }

        @Override
        protected AbstractScriptFieldType<?> buildFieldType() {
            String pattern = format.getValue() == null ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern() : format.getValue();
            Locale locale = this.locale.getValue() == null ? Locale.ROOT : this.locale.getValue();
            DateFormatter dateTimeFormatter = DateFormatter.forPattern(pattern).withLocale(locale);
            if (script.get() == null) {
                return new DateScriptFieldType(name, DateFieldScript.PARSE_FROM_SOURCE, dateTimeFormatter, this);
            }
            DateFieldScript.Factory factory = parserContext.scriptService().compile(script.getValue(), DateFieldScript.CONTEXT);
            return new DateScriptFieldType(name, factory, dateTimeFormatter, this);
        }
    });

    private final DateFormatter dateTimeFormatter;
    private final DateMathParser dateMathParser;

    private DateScriptFieldType(String name, DateFieldScript.Factory scriptFactory, DateFormatter dateTimeFormatter, Builder builder) {
        super(name, (n, params, ctx) -> scriptFactory.newFactory(n, params, ctx, dateTimeFormatter), builder);
        this.dateTimeFormatter = dateTimeFormatter;
        this.dateMathParser = dateTimeFormatter.toDateMathParser();
    }

    DateScriptFieldType(String name, DateFormatter dateTimeFormatter) {
        this(name, DateFieldScript.PARSE_FROM_SOURCE, dateTimeFormatter, null, Collections.emptyMap(), (builder, includeDefaults) -> {
            if (DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern().equals(dateTimeFormatter.pattern()) == false) {
                builder.field("format", dateTimeFormatter.pattern());
            }
        });
    }

    DateScriptFieldType(
        String name,
        DateFieldScript.Factory scriptFactory,
        DateFormatter dateTimeFormatter,
        Script script,
        Map<String, String> meta,
        CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent
    ) {
        super(name, (n, params, ctx) -> scriptFactory.newFactory(n, params, ctx, dateTimeFormatter), script, meta, toXContent);
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
    public Query termsQuery(List<?> values, SearchExecutionContext context) {
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
