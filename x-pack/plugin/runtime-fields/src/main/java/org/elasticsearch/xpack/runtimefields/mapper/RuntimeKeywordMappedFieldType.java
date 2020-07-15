/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptBinaryFieldData;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldFuzzyQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldPrefixQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldRangeQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldRegexpQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldTermQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldTermsQuery;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldWildcardQuery;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public final class RuntimeKeywordMappedFieldType extends MappedFieldType {

    private final Script script;
    private final StringScriptFieldScript.Factory scriptFactory;

    RuntimeKeywordMappedFieldType(String name, Script script, StringScriptFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, false, false, TextSearchInfo.NONE, meta);
        this.script = script;
        this.scriptFactory = scriptFactory;
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
    public String typeName() {
        // TODO not sure what we should return here: the runtime type or the field type?
        // why is the same string returned from three different methods?
        return ScriptFieldMapper.CONTENT_TYPE;
    }

    @Override
    public ScriptBinaryFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        // TODO once we get SearchLookup as an argument, we can already call scriptFactory.newFactory here and pass through the result
        return new ScriptBinaryFieldData.Builder(scriptFactory);
    }

    private StringScriptFieldScript.LeafFactory leafFactory(QueryShardContext context) {
        return scriptFactory.newFactory(script.getParams(), context.lookup());
    }

    private void checkAllowExpensiveQueries(QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new IllegalArgumentException(
                "queries cannot be executed against [script] fields while [" + ALLOW_EXPENSIVE_QUERIES.getKey() + "] is set to [false]."
            );
        }
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldExistsQuery(script, leafFactory(context), name());
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
            leafFactory(context),
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
        return new StringScriptFieldPrefixQuery(script, leafFactory(context), name(), value);
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        DateMathParser parser,
        QueryShardContext context
    ) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldRangeQuery(
            script,
            leafFactory(context),
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
        return new StringScriptFieldRegexpQuery(script, leafFactory(context), name(), value, flags, maxDeterminizedStates);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldTermQuery(script, leafFactory(context), name(), BytesRefs.toString(Objects.requireNonNull(value)));
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        Set<String> terms = values.stream().map(v -> BytesRefs.toString(Objects.requireNonNull(v))).collect(toSet());
        return new StringScriptFieldTermsQuery(script, leafFactory(context), name(), terms);
    }

    @Override
    public Query wildcardQuery(String value, RewriteMethod method, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new StringScriptFieldWildcardQuery(script, leafFactory(context), name(), value);
    }

    void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("runtime_type", "keyword");
        builder.field("script", script.getIdOrCode()); // TODO For some reason this doesn't allow us to do the full xcontent of the script.
    }
}
