/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.StringScriptFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.StringScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.StringScriptFieldFuzzyQuery;
import org.elasticsearch.search.runtime.StringScriptFieldPrefixQuery;
import org.elasticsearch.search.runtime.StringScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.StringScriptFieldRegexpQuery;
import org.elasticsearch.search.runtime.StringScriptFieldTermQuery;
import org.elasticsearch.search.runtime.StringScriptFieldTermsQuery;
import org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toSet;

public final class KeywordScriptFieldType extends AbstractScriptFieldType<StringFieldScript.LeafFactory> {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);

    private static class Builder extends AbstractScriptFieldType.Builder<StringFieldScript.Factory> {
        Builder(String name) {
            super(name, StringFieldScript.CONTEXT);
        }

        @Override
        protected AbstractScriptFieldType<?> createFieldType(
            String name,
            StringFieldScript.Factory factory,
            Script script,
            Map<String, String> meta,
            OnScriptError onScriptError
        ) {
            return new KeywordScriptFieldType(name, factory, script, meta, onScriptError);
        }

        @Override
        protected StringFieldScript.Factory getParseFromSourceFactory() {
            return StringFieldScript.PARSE_FROM_SOURCE;
        }

        @Override
        protected StringFieldScript.Factory getCompositeLeafFactory(
            Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory
        ) {
            return StringFieldScript.leafAdapter(parentScriptFactory);
        }
    }

    public static RuntimeField sourceOnly(String name) {
        return new Builder(name).createRuntimeField(StringFieldScript.PARSE_FROM_SOURCE);
    }

    public KeywordScriptFieldType(
        String name,
        StringFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        OnScriptError onScriptError
    ) {
        super(
            name,
            searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup, onScriptError),
            script,
            scriptFactory.isResultDeterministic(),
            meta
        );
    }

    @Override
    public String typeName() {
        return KeywordFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Object valueForDisplay(Object value) {
        if (value == null) {
            return null;
        }
        // keywords are internally stored as utf8 bytes
        BytesRef binaryValue = (BytesRef) value;
        return binaryValue.utf8ToString();
    }

    @Override
    public BlockLoader blockLoader(BlockLoaderContext blContext) {
        return new KeywordScriptBlockDocValuesReader.KeywordScriptBlockLoader(leafFactory(blContext.lookup()));
    }

    @Override
    public StringScriptFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        return new StringScriptFieldData.Builder(name(), leafFactory(fieldDataContext.lookupSupplier().get()), KeywordDocValuesField::new);
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        applyScriptContext(context);
        return new StringScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        SearchExecutionContext context,
        @Nullable MultiTermQuery.RewriteMethod rewriteMethod
    ) {
        applyScriptContext(context);
        return StringScriptFieldFuzzyQuery.build(
            script,
            leafFactory(context),
            name(),
            BytesRefs.toString(Objects.requireNonNull(value)),
            fuzziness.asDistance(BytesRefs.toString(value)),
            prefixLength,
            transpositions
        );
    }

    @Override
    public Query prefixQuery(String value, RewriteMethod method, boolean caseInsensitive, SearchExecutionContext context) {
        applyScriptContext(context);
        return new StringScriptFieldPrefixQuery(script, leafFactory(context), name(), value, caseInsensitive);
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
        return new StringScriptFieldRangeQuery(
            script,
            leafFactory(context),
            name(),
            lowerTerm == null ? null : BytesRefs.toString(lowerTerm),
            upperTerm == null ? null : BytesRefs.toString(upperTerm),
            includeLower,
            includeUpper
        );
    }

    @Override
    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        RewriteMethod method,
        SearchExecutionContext context
    ) {
        applyScriptContext(context);
        if (matchFlags != 0) {
            throw new IllegalArgumentException("Match flags not yet implemented [" + matchFlags + "]");
        }
        return new StringScriptFieldRegexpQuery(
            script,
            leafFactory(context),
            name(),
            value,
            syntaxFlags,
            matchFlags,
            maxDeterminizedStates
        );
    }

    @Override
    public Query termQueryCaseInsensitive(Object value, SearchExecutionContext context) {
        applyScriptContext(context);
        return new StringScriptFieldTermQuery(
            script,
            leafFactory(context),
            name(),
            BytesRefs.toString(Objects.requireNonNull(value)),
            true
        );
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        applyScriptContext(context);
        return new StringScriptFieldTermQuery(
            script,
            leafFactory(context),
            name(),
            BytesRefs.toString(Objects.requireNonNull(value)),
            false
        );
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        applyScriptContext(context);
        Set<String> terms = values.stream().map(v -> BytesRefs.toString(Objects.requireNonNull(v))).collect(toSet());
        return new StringScriptFieldTermsQuery(script, leafFactory(context), name(), terms);
    }

    @Override
    public Query wildcardQuery(String value, RewriteMethod method, boolean caseInsensitive, SearchExecutionContext context) {
        applyScriptContext(context);
        return new StringScriptFieldWildcardQuery(script, leafFactory(context), name(), value, caseInsensitive);
    }

    @Override
    public Query normalizedWildcardQuery(String value, RewriteMethod method, SearchExecutionContext context) {
        applyScriptContext(context);
        return new StringScriptFieldWildcardQuery(script, leafFactory(context), name(), value, false);
    }
}
