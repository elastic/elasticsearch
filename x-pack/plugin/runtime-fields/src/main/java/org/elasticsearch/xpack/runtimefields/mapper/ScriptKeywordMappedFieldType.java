/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptStringFieldData;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldFuzzyQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldPrefixQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldRangeQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldRegexpQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldTermQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldTermsQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldWildcardQuery;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

public final class ScriptKeywordMappedFieldType extends AbstractScriptMappedFieldType {

    private final Script script;
    private final StringScriptFieldScript.Factory scriptFactory;

    ScriptKeywordMappedFieldType(String name, Script script, StringScriptFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, meta);
        this.script = script;
        this.scriptFactory = scriptFactory;
    }

    @Override
    protected String runtimeType() {
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
    public ScriptStringFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new ScriptStringFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    private StringScriptFieldScript.LeafFactory leafFactory(SearchLookup searchLookup) {
        return scriptFactory.newFactory(name(), script.getParams(), searchLookup);
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldExistsQuery(script, leafFactory(context.lookup()), name());
    }

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        QueryShardContext context
    ) {
        checkAllowExpensiveQueries(context);
        return StringScriptFieldFuzzyQuery.build(
            script,
            leafFactory(context.lookup()),
            name(),
            BytesRefs.toString(Objects.requireNonNull(value)),
            fuzziness.asDistance(BytesRefs.toString(value)),
            prefixLength,
            transpositions
        );
    }

    @Override
    public Query prefixQuery(String value, RewriteMethod method, org.elasticsearch.index.query.QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldPrefixQuery(script, leafFactory(context.lookup()), name(), value);
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
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldRangeQuery(
            script,
            leafFactory(context.lookup()),
            name(),
            BytesRefs.toString(Objects.requireNonNull(lowerTerm)),
            BytesRefs.toString(Objects.requireNonNull(upperTerm)),
            includeLower,
            includeUpper
        );
    }

    @Override
    public Query regexpQuery(String value, int flags, int maxDeterminizedStates, RewriteMethod method, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldRegexpQuery(script, leafFactory(context.lookup()), name(), value, flags, maxDeterminizedStates);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldTermQuery(
            script,
            leafFactory(context.lookup()),
            name(),
            BytesRefs.toString(Objects.requireNonNull(value))
        );
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        Set<String> terms = values.stream().map(v -> BytesRefs.toString(Objects.requireNonNull(v))).collect(toSet());
        return new StringScriptFieldTermsQuery(script, leafFactory(context.lookup()), name(), terms);
    }

    @Override
    public Query wildcardQuery(String value, RewriteMethod method, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldWildcardQuery(script, leafFactory(context.lookup()), name(), value);
    }
}
