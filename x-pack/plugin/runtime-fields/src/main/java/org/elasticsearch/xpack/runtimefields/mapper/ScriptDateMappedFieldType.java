/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.runtimefields.AbstractLongScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.DateScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptDateFieldData;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldRangeQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldTermQuery;
import org.elasticsearch.xpack.runtimefields.query.LongScriptFieldTermsQuery;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class ScriptDateMappedFieldType extends AbstractScriptMappedFieldType {
    private final DateScriptFieldScript.Factory scriptFactory;

    ScriptDateMappedFieldType(String name, Script script, DateScriptFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, meta);
        this.scriptFactory = scriptFactory;
    }

    private DateFormatter dateTimeFormatter() {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;  // TODO make configurable
    }

    @Override
    protected String runtimeType() {
        return DateFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Object valueForDisplay(Object value) {
        Long val = (Long) value;
        if (val == null) {
            return null;
        }
        return dateTimeFormatter().format(Resolution.MILLISECONDS.toInstant(val).atZone(ZoneOffset.UTC));
    }

    @Override
    public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
        DateFormatter dateTimeFormatter = dateTimeFormatter();
        if (format != null) {
            dateTimeFormatter = DateFormatter.forPattern(format).withLocale(dateTimeFormatter.locale());
        }
        if (timeZone == null) {
            timeZone = ZoneOffset.UTC;
        }
        return new DocValueFormat.DateTime(dateTimeFormatter, timeZone, Resolution.MILLISECONDS);
    }

    @Override
    public ScriptDateFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        // TODO once we get SearchLookup as an argument, we can already call scriptFactory.newFactory here and pass through the result
        return new ScriptDateFieldData.Builder(name(), script, scriptFactory);
    }

    private AbstractLongScriptFieldScript.LeafFactory leafFactory(QueryShardContext context) {
        DateScriptFieldScript.LeafFactory delegate = scriptFactory.newFactory(script.getParams(), context.lookup());
        return ctx -> delegate.newInstance(ctx);
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new LongScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        @Nullable DateMathParser parser,
        QueryShardContext context
    ) {
        parser = parser == null ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser() : parser;
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
            (l, u) -> new LongScriptFieldRangeQuery(script, leafFactory(context), name(), l, u)
        );
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return DateFieldType.handleNow(context, now -> {
            long l = DateFieldType.parseToLong(
                value,
                false,
                null,
                dateTimeFormatter().toDateMathParser(),
                now,
                DateFieldMapper.Resolution.MILLISECONDS
            );
            checkAllowExpensiveQueries(context);
            return new LongScriptFieldTermQuery(script, leafFactory(context), name(), l);
        });
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchAllQuery();
        }
        return DateFieldType.handleNow(context, now -> {
            LongSet terms = new LongHashSet(values.size());
            for (Object value : values) {
                terms.add(
                    DateFieldType.parseToLong(
                        value,
                        false,
                        null,
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser(),
                        now,
                        DateFieldMapper.Resolution.MILLISECONDS
                    )
                );
            }
            checkAllowExpensiveQueries(context);
            return new LongScriptFieldTermsQuery(script, leafFactory(context), name(), terms);
        });
    }
}
