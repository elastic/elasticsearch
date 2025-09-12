/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.DateScriptFieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
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
import java.util.Set;
import java.util.function.Function;

public class DateScriptFieldType extends AbstractScriptFieldType<DateFieldScript.LeafFactory> {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);

    private static class Builder extends AbstractScriptFieldType.Builder<DateFieldScript.Factory> {
        private final FieldMapper.Parameter<String> format = FieldMapper.Parameter.stringParam(
            "format",
            true,
            RuntimeField.initializerNotSupported(),
            null,
            (b, n, v) -> {
                if (v != null && false == v.equals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern())) {
                    b.field(n, v);
                }
            }
        ).acceptsNull();

        private final FieldMapper.Parameter<Locale> locale = new FieldMapper.Parameter<>(
            "locale",
            true,
            () -> null,
            (n, c, o) -> o == null ? null : LocaleUtils.parse(o.toString()),
            RuntimeField.initializerNotSupported(),
            (b, n, v) -> {
                if (v != null && false == v.equals(DateFieldMapper.DEFAULT_LOCALE)) {
                    b.field(n, v.toString());
                }
            },
            Object::toString
        ).acceptsNull();

        protected Builder(String name) {
            super(name, DateFieldScript.CONTEXT);
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(format);
            parameters.add(locale);
            return Collections.unmodifiableList(parameters);
        }

        protected AbstractScriptFieldType<?> createFieldType(
            String name,
            DateFieldScript.Factory factory,
            Script script,
            Map<String, String> meta,
            IndexVersion supportedVersion,
            OnScriptError onScriptError
        ) {
            String pattern = format.getValue() == null ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern() : format.getValue();
            Locale locale = this.locale.getValue() == null ? DateFieldMapper.DEFAULT_LOCALE : this.locale.getValue();
            DateFormatter dateTimeFormatter = DateFormatter.forPattern(pattern, supportedVersion).withLocale(locale);
            return new DateScriptFieldType(name, factory, dateTimeFormatter, script, meta, onScriptError);
        }

        @Override
        protected AbstractScriptFieldType<?> createFieldType(
            String name,
            DateFieldScript.Factory factory,
            Script script,
            Map<String, String> meta,
            OnScriptError onScriptError
        ) {
            return createFieldType(name, factory, script, meta, IndexVersion.current(), onScriptError);
        }

        @Override
        protected DateFieldScript.Factory getParseFromSourceFactory() {
            return DateFieldScript.PARSE_FROM_SOURCE;
        }

        @Override
        protected DateFieldScript.Factory getCompositeLeafFactory(
            Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory
        ) {
            return DateFieldScript.leafAdapter(parentScriptFactory);
        }
    }

    public static RuntimeField sourceOnly(String name, DateFormatter dateTimeFormatter, IndexVersion supportedVersion) {
        Builder builder = new Builder(name);
        builder.format.setValue(dateTimeFormatter.pattern());
        return builder.createRuntimeField(DateFieldScript.PARSE_FROM_SOURCE, supportedVersion);
    }

    private final DateFormatter dateTimeFormatter;
    private final DateMathParser dateMathParser;

    DateScriptFieldType(
        String name,
        DateFieldScript.Factory scriptFactory,
        DateFormatter dateTimeFormatter,
        Script script,
        Map<String, String> meta,
        OnScriptError onScriptError
    ) {
        super(
            name,
            searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup, dateTimeFormatter, onScriptError),
            script,
            scriptFactory.isResultDeterministic(),
            meta
        );
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
    public BlockLoader blockLoader(BlockLoaderContext blContext) {
        return new DateScriptBlockDocValuesReader.DateScriptBlockLoader(leafFactory(blContext.lookup()));
    }

    @Override
    public DateScriptFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        return new DateScriptFieldData.Builder(
            name(),
            leafFactory(fieldDataContext.lookupSupplier().get()),
            Resolution.MILLISECONDS.getDefaultToScriptFieldFactory()
        );
    }

    @Override
    public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
        applyScriptContext(context);
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
        @Nullable DateMathParser parser,
        SearchExecutionContext context
    ) {
        parser = parser == null ? this.dateMathParser : parser;
        applyScriptContext(context);
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
            applyScriptContext(context);
            return new LongScriptFieldTermQuery(script, leafFactory(context)::newInstance, name(), l);
        });
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchAllQuery();
        }
        return DateFieldType.handleNow(context, now -> {
            Set<Long> terms = Sets.newHashSetWithExpectedSize(values.size());
            for (Object value : values) {
                terms.add(DateFieldType.parseToLong(value, false, null, this.dateMathParser, now, DateFieldMapper.Resolution.MILLISECONDS));
            }
            applyScriptContext(context);
            return new LongScriptFieldTermsQuery(script, leafFactory(context)::newInstance, name(), terms);
        });
    }
}
