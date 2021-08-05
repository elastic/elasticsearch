/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Abstract base {@linkplain MappedFieldType} for runtime fields based on a script.
 */
abstract class AbstractScriptFieldType<LeafFactory> extends MappedFieldType {

    protected final Script script;
    private final Function<SearchLookup, LeafFactory> factory;

    AbstractScriptFieldType(
        String name,
        Function<SearchLookup, LeafFactory> factory,
        Script script,
        Map<String, String> meta
    ) {
        super(name, false, false, false, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
        this.factory = factory;
        this.script = Objects.requireNonNull(script);
    }

    @Override
    public final boolean isSearchable() {
        return true;
    }

    @Override
    public final boolean isAggregatable() {
        return true;
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
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context) {
        throw new IllegalArgumentException(unsupported("phrase", "text"));
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context) {
        throw new IllegalArgumentException(unsupported("phrase", "text"));
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) {
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
    public final ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        return new DocValueFetcher(docValueFormat(format, null), context.getForField(this));
    }

    /**
     * Create a script leaf factory.
     */
    protected final LeafFactory leafFactory(SearchLookup searchLookup) {
        return factory.apply(searchLookup);
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

    // Placeholder Script for source-only fields
    // TODO rework things so that we don't need this
    protected static final Script DEFAULT_SCRIPT = new Script("");

    abstract static class Builder<Factory> extends RuntimeField.Builder {
        private final ScriptContext<Factory> scriptContext;

        final FieldMapper.Parameter<Script> script = new FieldMapper.Parameter<>(
            "script",
            true,
            () -> null,
            Builder::parseScript,
            RuntimeField.initializerNotSupported()
        ).setSerializerCheck((id, ic, v) -> ic);

        Builder(String name, ScriptContext<Factory> scriptContext) {
            super(name);
            this.scriptContext = scriptContext;
        }

        abstract Factory getParseFromSourceFactory();

        abstract Factory getCompositeLeafFactory(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory);

        @Override
        protected RuntimeField createRuntimeField(MappingParserContext parserContext) {
            if (script.get() == null) {
                return new LeafRuntimeField(
                    name,
                    createFieldType(name, getParseFromSourceFactory(), getScript(), meta()),
                    getParameters()
                );
            }
            Factory factory = parserContext.scriptCompiler().compile(script.getValue(), scriptContext);
            return new LeafRuntimeField(name, createFieldType(name, factory, getScript(), meta()), getParameters());
        }

        @Override
        protected final RuntimeField createChildRuntimeField(MappingParserContext parserContext,
                                                        String parent,
                                                        Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory) {
            if (script.isConfigured()) {
                throw new IllegalArgumentException("Cannot use [script] parameter on sub-field [" + name +
                    "] of composite field [" + parent + "]");
            }
            String fullName = parent + "." + name;
            return new LeafRuntimeField(
                name,
                createFieldType(fullName, getCompositeLeafFactory(parentScriptFactory), getScript(), meta()),
                getParameters()
            );
        }

        final RuntimeField createRuntimeField(String parent, Factory scriptFactory) {
            String fullName = parent == null ? name : parent + "." + name;
            AbstractScriptFieldType<?> fieldType = createFieldType(fullName, scriptFactory, getScript(), meta());
            return new LeafRuntimeField(name, fieldType, getParameters());
        }

        abstract AbstractScriptFieldType<?> createFieldType(String name, Factory factory, Script script, Map<String, String> meta);

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(script);
            return Collections.unmodifiableList(parameters);
        }

        protected final Script getScript() {
            if (script.get() == null) {
                return DEFAULT_SCRIPT;
            }
            return script.get();
        }

        static Script parseScript(String name, MappingParserContext parserContext, Object scriptObject) {
            Script script = Script.parse(scriptObject);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException("stored scripts are not supported for runtime field [" + name + "]");
            }
            return script;
        }
    }
}
