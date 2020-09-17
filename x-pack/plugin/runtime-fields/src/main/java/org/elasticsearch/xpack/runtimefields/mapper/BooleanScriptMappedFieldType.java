/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.fielddata.BooleanScriptFieldData;
import org.elasticsearch.xpack.runtimefields.query.BooleanScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.BooleanScriptFieldTermQuery;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class BooleanScriptMappedFieldType extends AbstractScriptMappedFieldType<BooleanFieldScript.LeafFactory> {
    BooleanScriptMappedFieldType(String name, Script script, BooleanFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, scriptFactory::newFactory, meta);
    }

    @Override
    protected String runtimeType() {
        return BooleanFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Object valueForDisplay(Object value) {
        if (value == null) {
            return null;
        }
        switch (value.toString()) {
            case "F":
                return false;
            case "T":
                return true;
            default:
                throw new IllegalArgumentException("Expected [T] or [F] but got [" + value + "]");
        }
    }

    @Override
    public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
        }
        if (timeZone != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
        }
        return DocValueFormat.BOOLEAN;
    }

    @Override
    public BooleanScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new BooleanScriptFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new BooleanScriptFieldExistsQuery(script, leafFactory(context), name());
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
        boolean trueAllowed;
        boolean falseAllowed;

        /*
         * gte: true --- true matches
         * gt: true ---- none match
         * gte: false -- both match
         * gt: false --- true matches
         */
        if (toBoolean(lowerTerm)) {
            if (includeLower) {
                trueAllowed = true;
                falseAllowed = false;
            } else {
                trueAllowed = false;
                falseAllowed = false;
            }
        } else {
            if (includeLower) {
                trueAllowed = true;
                falseAllowed = true;
            } else {
                trueAllowed = true;
                falseAllowed = false;
            }
        }

        /*
         * This is how the indexed version works:
         * lte: true --- both match
         * lt: true ---- false matches
         * lte: false -- false matches
         * lt: false --- none match
         */
        if (toBoolean(upperTerm)) {
            if (includeUpper) {
                trueAllowed &= true;
                falseAllowed &= true;
            } else {
                trueAllowed &= false;
                falseAllowed &= true;
            }
        } else {
            if (includeUpper) {
                trueAllowed &= false;
                falseAllowed &= true;
            } else {
                trueAllowed &= false;
                falseAllowed &= false;
            }
        }

        return termsQuery(trueAllowed, falseAllowed, context);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new BooleanScriptFieldTermQuery(script, leafFactory(context), name(), toBoolean(value));
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchNoDocsQuery("Empty terms query");
        }
        boolean trueAllowed = false;
        boolean falseAllowed = false;
        for (Object value : values) {
            if (toBoolean(value)) {
                trueAllowed = true;
            } else {
                falseAllowed = true;
            }
        }
        return termsQuery(trueAllowed, falseAllowed, context);
    }

    private Query termsQuery(boolean trueAllowed, boolean falseAllowed, QueryShardContext context) {
        if (trueAllowed) {
            if (falseAllowed) {
                // Either true or false
                return existsQuery(context);
            }
            checkAllowExpensiveQueries(context);
            return new BooleanScriptFieldTermQuery(script, leafFactory(context), name(), true);
        }
        if (falseAllowed) {
            checkAllowExpensiveQueries(context);
            return new BooleanScriptFieldTermQuery(script, leafFactory(context), name(), false);
        }
        return new MatchNoDocsQuery("neither true nor false allowed");
    }

    /**
     * Convert the term into a boolean. Inspired by {@link BooleanFieldMapper.BooleanFieldType#indexedValueForSearch(Object)}.
     */
    private static boolean toBoolean(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String sValue;
        if (value instanceof BytesRef) {
            sValue = ((BytesRef) value).utf8ToString();
        } else {
            sValue = value.toString();
        }
        return Booleans.parseBoolean(sValue);
    }
}
