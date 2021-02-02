/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Abstract base {@linkplain MappedFieldType} for scripted fields.
 */
abstract class AbstractScriptFieldType<LeafFactory> extends RuntimeFieldType {
    protected final Script script;
    private final TriFunction<String, Map<String, Object>, SearchLookup, LeafFactory> factory;
    private final CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent;

    AbstractScriptFieldType(String name, TriFunction<String, Map<String, Object>, SearchLookup, LeafFactory> factory, Builder builder) {
        this(name, factory, builder.getScript(), builder.meta.getValue(), builder::toXContent);
    }

    AbstractScriptFieldType(
        String name,
        TriFunction<String, Map<String, Object>, SearchLookup, LeafFactory> factory,
        Script script,
        Map<String, String> meta,
        CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent
    ) {
        super(name, meta);
        this.factory = factory;
        this.script = script;
        this.toXContent = toXContent;
    }

    @Override
    public final boolean isSearchable() {
        return true;
    }

    @Override
    public final boolean isAggregatable() {
        return true;
    }

    /**
     * Create a script leaf factory.
     */
    protected final LeafFactory leafFactory(SearchLookup searchLookup) {
        return factory.apply(name(), script.getParams(), searchLookup);
    }

    /**
     * Create a script leaf factory for queries.
     */
    protected final LeafFactory leafFactory(SearchExecutionContext context) {
        /*
         * Forking here causes us to count this field in the field data loop
         * detection code as though we were resolving field data for this field.
         * We're not, but running the query is close enough.
         */
        return leafFactory(context.lookup().forkAndTrackFieldReferences(name()));
    }

    @Override
    public final Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
    ) {
        if (relation == ShapeRelation.DISJOINT) {
            String message = "Runtime field [%s] of type [%s] does not support DISJOINT ranges";
            throw new IllegalArgumentException(String.format(Locale.ROOT, message, name(), typeName()));
        }
        return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, parser, context);
    }

    protected abstract Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
    );

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException(unsupported("fuzzy", "keyword and text"));
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, SearchExecutionContext context) {
        throw new IllegalArgumentException(unsupported("prefix", "keyword, text and wildcard"));
    }

    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, SearchExecutionContext context) {
        throw new IllegalArgumentException(unsupported("wildcard", "keyword, text and wildcard"));
    }

    @Override
    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        MultiTermQuery.RewriteMethod method,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException(unsupported("regexp", "keyword and text"));
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) {
        throw new IllegalArgumentException(unsupported("phrase", "text"));
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) {
        throw new IllegalArgumentException(unsupported("phrase", "text"));
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) {
        throw new IllegalArgumentException(unsupported("phrase prefix", "text"));
    }

    @Override
    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
        throw new IllegalArgumentException(unsupported("span prefix", "text"));
    }

    private String unsupported(String query, String supported) {
        return String.format(
            Locale.ROOT,
            "Can only use %s queries on %s fields - not on [%s] which is a runtime field of type [%s]",
            query,
            supported,
            name(),
            typeName()
        );
    }

    protected final void checkAllowExpensiveQueries(SearchExecutionContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "queries cannot be executed against runtime fields while [" + ALLOW_EXPENSIVE_QUERIES.getKey() + "] is set to [false]."
            );
        }
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        return new DocValueFetcher(docValueFormat(format, null), context.getForField(this));
    }

    @Override
    protected final void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
        toXContent.accept(builder, includeDefaults);
    }

    // Placeholder Script for source-only fields
    // TODO rework things so that we don't need this
    private static final Script DEFAULT_SCRIPT = new Script("");

    /**
     *  For runtime fields the {@link RuntimeFieldType.Parser} returns directly the {@link MappedFieldType}.
     *  Internally we still create a {@link Builder} so we reuse the {@link FieldMapper.Parameter} infrastructure,
     *  but {@link Builder#init(FieldMapper)} and {@link Builder#build(ContentPath)} are never called as
     *  {@link RuntimeFieldTypeParser#parse(String, Map, Mapper.TypeParser.ParserContext)} calls
     *  {@link Builder#parse(String, Mapper.TypeParser.ParserContext, Map)} and returns the corresponding
     *  {@link MappedFieldType}.
     */
    abstract static class Builder extends FieldMapper.Builder {
        final FieldMapper.Parameter<Map<String, String>> meta = FieldMapper.Parameter.metaParam();
        final FieldMapper.Parameter<Script> script = new FieldMapper.Parameter<>(
            "script",
            true,
            () -> null,
            Builder::parseScript,
            initializerNotSupported()
        ).setSerializerCheck((id, ic, v) -> ic);

        Builder(String name) {
            super(name);
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            return List.of(meta, script);
        }

        protected abstract AbstractScriptFieldType<?> buildFieldType();

        protected final Script getScript() {
            if (script.get() == null) {
                return DEFAULT_SCRIPT;
            }
            return script.get();
        }

        @Override
        public FieldMapper.Builder init(FieldMapper initializer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldMapper build(ContentPath context) {
            throw new UnsupportedOperationException();
        }

        private void validate() {
            ContentPath contentPath = parentPath(name());
            FieldMapper.MultiFields multiFields = multiFieldsBuilder.build(this, contentPath);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException("runtime field [" + name + "] does not support [fields]");
            }
            FieldMapper.CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException("runtime field [" + name + "] does not support [copy_to]");
            }
        }

        private static Script parseScript(String name, Mapper.TypeParser.ParserContext parserContext, Object scriptObject) {
            Script script = Script.parse(scriptObject);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException("stored scripts are not supported for runtime field [" + name + "]");
            }
            return script;
        }
    }

    static <T> Function<FieldMapper, T> initializerNotSupported() {
        return mapper -> { throw new UnsupportedOperationException(); };
    }

    static final class RuntimeFieldTypeParser implements RuntimeFieldType.Parser {
        private final BiFunction<String, Mapper.TypeParser.ParserContext, Builder> builderFunction;

        RuntimeFieldTypeParser(BiFunction<String, Mapper.TypeParser.ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public RuntimeFieldType parse(String name, Map<String, Object> node, Mapper.TypeParser.ParserContext parserContext)
            throws MapperParsingException {

            Builder builder = builderFunction.apply(name, parserContext);
            builder.parse(name, parserContext, node);
            builder.validate();
            return builder.buildFieldType();
        }
    }
}
