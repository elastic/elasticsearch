/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;

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
public abstract class AbstractScriptFieldType<LeafFactory> extends MappedFieldType {

    protected final Script script;
    private final Function<SearchLookup, LeafFactory> factory;
    private final boolean isResultDeterministic;

    protected AbstractScriptFieldType(
        String name,
        Function<SearchLookup, LeafFactory> factory,
        Script script,
        boolean isResultDeterministic,
        Map<String, String> meta
    ) {
        super(name, false, false, false, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
        this.factory = factory;
        this.script = Objects.requireNonNull(script);
        this.isResultDeterministic = isResultDeterministic;
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
        SearchExecutionContext context,
        @Nullable MultiTermQuery.RewriteMethod rewriteMethod
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

    protected final void applyScriptContext(SearchExecutionContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "queries cannot be executed against runtime fields while [" + ALLOW_EXPENSIVE_QUERIES.getKey() + "] is set to [false]."
            );
        }
        if (isResultDeterministic == false) {
            context.disableCache();
        }
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        return new DocValueFetcher(
            docValueFormat(format, null),
            context.getForField(this, FielddataOperation.SEARCH),
            StoredFieldsSpec.NEEDS_SOURCE       // for now we assume runtime fields need source
        );
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

    @Override
    public void validateMatchedRoutingPath(final String routingPath) {
        throw new IllegalArgumentException(
            "All fields that match routing_path "
                + "must be configured with [time_series_dimension: true] "
                + "or flattened fields with a list of dimensions in [time_series_dimensions] "
                + "and without the [script] parameter. ["
                + name()
                + "] was a runtime ["
                + typeName()
                + "]."
        );
    }

    @Override
    public final boolean fieldHasValue(FieldInfos fieldInfos) {
        // To know whether script field types have value we would need to run the script,
        // this because script fields do not have footprint in Lucene. Since running the
        // script would be too expensive for _field_caps we consider them as always non-empty.
        return true;
    }

    // Placeholder Script for source-only fields
    // TODO rework things so that we don't need this
    protected static final Script DEFAULT_SCRIPT = new Script("");

    protected abstract static class Builder<Factory> extends RuntimeField.Builder {
        private final ScriptContext<Factory> scriptContext;

        private final FieldMapper.Parameter<Script> script = new FieldMapper.Parameter<>(
            "script",
            true,
            () -> null,
            RuntimeField::parseScript,
            RuntimeField.initializerNotSupported(),
            XContentBuilder::field,
            Objects::toString
        ).setSerializerCheck((id, ic, v) -> ic);

        private final FieldMapper.Parameter<OnScriptError> onScriptError = FieldMapper.Parameter.onScriptErrorParam(
            m -> m.builderParams.onScriptError(),
            script
        );

        protected Builder(String name, ScriptContext<Factory> scriptContext) {
            super(name);
            this.scriptContext = scriptContext;
        }

        protected abstract Factory getParseFromSourceFactory();

        protected abstract Factory getCompositeLeafFactory(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory);

        @Override
        protected final RuntimeField createRuntimeField(MappingParserContext parserContext) {
            if (script.get() == null) {
                return createRuntimeField(getParseFromSourceFactory(), parserContext.indexVersionCreated());
            }
            Factory factory = parserContext.scriptCompiler().compile(script.getValue(), scriptContext);
            return createRuntimeField(factory, parserContext.indexVersionCreated());
        }

        @Override
        protected final RuntimeField createChildRuntimeField(
            MappingParserContext parserContext,
            String parent,
            Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory,
            OnScriptError onScriptError
        ) {
            if (script.isConfigured()) {
                throw new IllegalArgumentException(
                    "Cannot use [script] parameter on sub-field [" + name + "] of composite field [" + parent + "]"
                );
            }
            String fullName = parent + "." + name;
            return new LeafRuntimeField(
                name,
                createFieldType(fullName, getCompositeLeafFactory(parentScriptFactory), getScript(), meta(), onScriptError),
                getParameters()
            );
        }

        final RuntimeField createRuntimeField(Factory scriptFactory) {
            return createRuntimeField(scriptFactory, IndexVersion.current());
        }

        final RuntimeField createRuntimeField(Factory scriptFactory, IndexVersion indexVersion) {
            var fieldType = createFieldType(name, scriptFactory, getScript(), meta(), indexVersion, onScriptError.get());
            return new LeafRuntimeField(name, fieldType, getParameters());
        }

        protected abstract AbstractScriptFieldType<?> createFieldType(
            String name,
            Factory factory,
            Script script,
            Map<String, String> meta,
            OnScriptError onScriptError
        );

        protected AbstractScriptFieldType<?> createFieldType(
            String name,
            Factory factory,
            Script script,
            Map<String, String> meta,
            IndexVersion supportedVersion,
            OnScriptError onScriptError
        ) {
            return createFieldType(name, factory, script, meta, onScriptError);
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(script);
            parameters.add(onScriptError);
            return Collections.unmodifiableList(parameters);
        }

        protected final Script getScript() {
            if (script.get() == null) {
                return DEFAULT_SCRIPT;
            }
            return script.get();
        }

    }
}
