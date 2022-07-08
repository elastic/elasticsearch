/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.fielddata.BooleanScriptFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.field.BooleanDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.BooleanScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.BooleanScriptFieldTermQuery;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class BooleanScriptFieldType extends AbstractScriptFieldType<BooleanFieldScript.LeafFactory> {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);

    private static class Builder extends AbstractScriptFieldType.Builder<BooleanFieldScript.Factory> {
        Builder(String name) {
            super(name, BooleanFieldScript.CONTEXT);
        }

        @Override
        AbstractScriptFieldType<?> createFieldType(
            BooleanFieldScript.Factory factory,
            Script script,
            Map<String, String> meta
        ) {
            return new BooleanScriptFieldType(factory, script, meta);
        }

        @Override
        BooleanFieldScript.Factory getParseFromSourceFactory() {
            return BooleanFieldScript.PARSE_FROM_SOURCE;
        }

        @Override
        BooleanFieldScript.Factory getCompositeLeafFactory(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory) {
            return BooleanFieldScript.leafAdapter(parentScriptFactory);
        }

    }

    public static RuntimeField sourceOnly(String name) {
        return new Builder(name).createRuntimeField(BooleanFieldScript.PARSE_FROM_SOURCE);
    }

    BooleanScriptFieldType(BooleanFieldScript.Factory scriptFactory, Script script, Map<String, String> meta) {
        super(
            (name, searchLookup) -> scriptFactory.newFactory(name, script.getParams(), searchLookup),
            script,
            scriptFactory.isResultDeterministic(),
            meta
        );
    }

    @Override
    public String typeName() {
        return BooleanFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Object valueForDisplay(Object value) {
        if (value == null) {
            return null;
        }
        return switch (value.toString()) {
            case "F" -> false;
            case "T" -> true;
            default -> throw new IllegalArgumentException("Expected [T] or [F] but got [" + value + "]");
        };
    }

    @Override
    public DocValueFormat docValueFormat(String name, String format, ZoneId timeZone) {
        checkNoFormat(name, format);
        checkNoTimeZone(name, timeZone);
        return DocValueFormat.BOOLEAN;
    }

    @Override
    public BooleanScriptFieldData.Builder fielddataBuilder(String name, String fullyQualifiedIndexName,
                                                           Supplier<SearchLookup> searchLookup) {
        return new BooleanScriptFieldData.Builder(name, leafFactory(name, searchLookup.get()), BooleanDocValuesField::new);
    }

    @Override
    public Query existsQuery(String name, SearchExecutionContext context) {
        applyScriptContext(context);
        return new BooleanScriptFieldExistsQuery(script, leafFactory(name, context), name);
    }

    @Override
    public Query rangeQuery(
        String name,
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
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

        return termsQuery(name, trueAllowed, falseAllowed, context);
    }

    @Override
    public Query termQueryCaseInsensitive(String name, Object value, SearchExecutionContext context) {
        applyScriptContext(context);
        return new BooleanScriptFieldTermQuery(script, leafFactory(name, context.lookup()), name, toBoolean(value, true));
    }

    @Override
    public Query termQuery(String name, Object value, SearchExecutionContext context) {
        applyScriptContext(context);
        return new BooleanScriptFieldTermQuery(script, leafFactory(name, context), name, toBoolean(value, false));
    }

    @Override
    public Query termsQuery(String name, Collection<?> values, SearchExecutionContext context) {
        if (values.isEmpty()) {
            return Queries.newMatchNoDocsQuery("Empty terms query");
        }
        boolean trueAllowed = false;
        boolean falseAllowed = false;
        for (Object value : values) {
            if (toBoolean(value, false)) {
                trueAllowed = true;
            } else {
                falseAllowed = true;
            }
        }
        return termsQuery(name, trueAllowed, falseAllowed, context);
    }

    private Query termsQuery(String name, boolean trueAllowed, boolean falseAllowed, SearchExecutionContext context) {
        if (trueAllowed) {
            if (falseAllowed) {
                // Either true or false
                return existsQuery(name, context);
            }
            applyScriptContext(context);
            return new BooleanScriptFieldTermQuery(script, leafFactory(name, context), name, true);
        }
        if (falseAllowed) {
            applyScriptContext(context);
            return new BooleanScriptFieldTermQuery(script, leafFactory(name, context), name, false);
        }
        return new MatchNoDocsQuery("neither true nor false allowed");
    }

    private static boolean toBoolean(Object value) {
        return toBoolean(value, false);
    }

    /**
     * Convert the term into a boolean. Inspired by {@link BooleanFieldMapper.BooleanFieldType#indexedValueForSearch(String, Object)}.
     */
    private static boolean toBoolean(Object value, boolean caseInsensitive) {
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
        if (caseInsensitive) {
            sValue = Strings.toLowercaseAscii(sValue);
        }
        return Booleans.parseBoolean(sValue);
    }
}
