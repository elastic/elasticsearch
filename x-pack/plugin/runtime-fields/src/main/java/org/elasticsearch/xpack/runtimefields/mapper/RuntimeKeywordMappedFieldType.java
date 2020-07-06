/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.StringRuntimeValues;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptBinaryFieldData;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public final class RuntimeKeywordMappedFieldType extends MappedFieldType {

    private final Script script;
    private final StringScriptFieldScript.Factory scriptFactory;

    RuntimeKeywordMappedFieldType(String name, Script script, StringScriptFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
        this.script = script;
        this.scriptFactory = scriptFactory;
    }

    RuntimeKeywordMappedFieldType(RuntimeKeywordMappedFieldType ref) {
        super(ref);
        this.script = ref.script;
        this.scriptFactory = ref.scriptFactory;
    }

    @Override
    public MappedFieldType clone() {
        return new RuntimeKeywordMappedFieldType(this);
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

    private StringRuntimeValues runtimeValues(QueryShardContext ctx) {
        // TODO cache the runtimeValues in the context somehow
        return scriptFactory.newFactory(script.getParams(), ctx.lookup()).runtimeValues();
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        // TODO once we get SearchLookup as an argument, we can already call scriptFactory.newFactory here and pass through the result
        return new ScriptBinaryFieldData.Builder(scriptFactory);
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        return runtimeValues(context).existsQuery(name());
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
        String term = BytesRefs.toString(value);
        return runtimeValues(context).fuzzyQuery(name(), term, fuzziness.asDistance(term), prefixLength, maxExpansions, transpositions);
    }

    @Override
    public Query prefixQuery(String value, RewriteMethod method, QueryShardContext context) {
        return runtimeValues(context).prefixQuery(name(), value);
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
        return runtimeValues(context).rangeQuery(
            name(),
            BytesRefs.toString(lowerTerm),
            BytesRefs.toString(upperTerm),
            includeLower,
            includeUpper
        );
    }

    @Override
    public Query regexpQuery(String value, int flags, int maxDeterminizedStates, RewriteMethod method, QueryShardContext context) {
        return runtimeValues(context).regexpQuery(name(), value, flags, maxDeterminizedStates);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return runtimeValues(context).termQuery(name(), BytesRefs.toString(value));
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        return runtimeValues(context).termsQuery(name(), values.stream().map(BytesRefs::toString).toArray(String[]::new));
    }

    @Override
    public Query wildcardQuery(String value, RewriteMethod method, QueryShardContext context) {
        return runtimeValues(context).wildcardQuery(name(), value);
    }

    void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("runtime_type", "keyword");
        builder.field("script", script.getIdOrCode()); // TODO For some reason this doesn't allow us to do the full xcontent of the script.
    }

    // TODO do we need to override equals/hashcode?
}
