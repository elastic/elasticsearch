/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Plugin for extending search time behavior.
 */
public interface SearchPlugin {
    /**
     * The new {@link ScoreFunction}s defined by this plugin.
     */
    default List<ScoreFunctionSpec<?>> getScoreFunctions() {
        return emptyList();
    }

    /**
     * The new {@link SignificanceHeuristic}s defined by this plugin. {@linkplain SignificanceHeuristic}s are used by the
     * {@link SignificantTerms} aggregation to pick which terms are significant for a given query.
     */
    default List<SignificanceHeuristicSpec<?>> getSignificanceHeuristics() {
        return emptyList();
    }

    /**
     * The new {@link QueryVectorBuilder}s defined by this plugin. {@linkplain QueryVectorBuilder}s can be used within a kNN
     * search to build the query vector instead of having the user provide the vector directly
     */
    default List<QueryVectorBuilderSpec<?>> getQueryVectorBuilders() {
        return emptyList();
    }

    /**
     * The new {@link FetchSubPhase}s defined by this plugin.
     */
    default List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return emptyList();
    }

    /**
     * The new {@link SearchExtBuilder}s defined by this plugin.
     */
    default List<SearchExtSpec<?>> getSearchExts() {
        return emptyList();
    }

    /**
     * Get the {@link Highlighter}s defined by this plugin.
     */
    default Map<String, Highlighter> getHighlighters() {
        return emptyMap();
    }

    /**
     * The new {@link Suggester}s defined by this plugin.
     */
    default List<SuggesterSpec<?>> getSuggesters() {
        return emptyList();
    }

    /**
     * The new {@link Query}s defined by this plugin.
     */
    default List<QuerySpec<?>> getQueries() {
        return emptyList();
    }

    /**
     * The new {@link Aggregation}s added by this plugin.
     */
    default List<AggregationSpec> getAggregations() {
        return emptyList();
    }

    /**
     * Allows plugins to register new aggregations using aggregation names that are already defined
     * in Core, as long as the new aggregations target different ValuesSourceTypes
     * @return A list of the new registrar functions
     */
    default List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return emptyList();
    }

    /**
     * The new {@link PipelineAggregator}s added by this plugin.
     */
    default List<PipelineAggregationSpec> getPipelineAggregations() {
        return emptyList();
    }

    /**
     * The new {@link Rescorer}s added by this plugin.
     */
    default List<RescorerSpec<?>> getRescorers() {
        return emptyList();
    }

    /**
     * Allows plugins to register a cache differentiator which contributes to the cacheKey
     * computation for the request cache. This helps differentiate between queries that
     * are otherwise identical.
     */
    @Nullable
    default CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> getRequestCacheKeyDifferentiator() {
        return null;
    }

    /**
     * Specification of custom {@link ScoreFunction}.
     */
    class ScoreFunctionSpec<T extends ScoreFunctionBuilder<T>> extends SearchExtensionSpec<T, ScoreFunctionParser<T>> {
        public ScoreFunctionSpec(ParseField name, Writeable.Reader<T> reader, ScoreFunctionParser<T> parser) {
            super(name, reader, parser);
        }

        public ScoreFunctionSpec(String name, Writeable.Reader<T> reader, ScoreFunctionParser<T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification of custom {@link SignificanceHeuristic}.
     */
    class SignificanceHeuristicSpec<T extends SignificanceHeuristic> extends SearchExtensionSpec<T, BiFunction<XContentParser, Void, T>> {
        public SignificanceHeuristicSpec(ParseField name, Writeable.Reader<T> reader, BiFunction<XContentParser, Void, T> parser) {
            super(name, reader, parser);
        }

        public SignificanceHeuristicSpec(String name, Writeable.Reader<T> reader, BiFunction<XContentParser, Void, T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification for a {@link Suggester}.
     */
    class SuggesterSpec<T extends SuggestionBuilder<T>> extends SearchExtensionSpec<T, CheckedFunction<XContentParser, T, IOException>> {

        private Writeable.Reader<? extends Suggest.Suggestion<?>> suggestionReader;

        /**
         * Specification of custom {@link Suggester}.
         *
         * @param name holds the names by which this suggester might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the request builder and Suggestion response readers are registered. So it is the name that the
         *        query and Suggestion response should use as their {@link NamedWriteable#getWriteableName()} return values too.
         * @param builderReader the reader registered for this suggester's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param builderParser a parser that reads the suggester's builder from xcontent
         * @param suggestionReader the reader registered for this suggester's Suggestion response. Typically a reference to a constructor
         *        that takes a {@link StreamInput}
         */
        public SuggesterSpec(
            ParseField name,
            Writeable.Reader<T> builderReader,
            CheckedFunction<XContentParser, T, IOException> builderParser,
            Writeable.Reader<? extends Suggest.Suggestion<?>> suggestionReader
        ) {

            super(name, builderReader, builderParser);
            setSuggestionReader(suggestionReader);
        }

        /**
         * Specification of custom {@link Suggester}.
         *
         * @param name the name by which this suggester might be parsed or deserialized. Make sure that the query builder and Suggestion
         *        response reader return this name for {@link NamedWriteable#getWriteableName()}.
         * @param builderReader the reader registered for this suggester's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param builderParser a parser that reads the suggester's builder from xcontent
         * @param suggestionReader the reader registered for this suggester's Suggestion response. Typically a reference to a constructor
         *        that takes a {@link StreamInput}
         */
        public SuggesterSpec(
            String name,
            Writeable.Reader<T> builderReader,
            CheckedFunction<XContentParser, T, IOException> builderParser,
            Writeable.Reader<? extends Suggest.Suggestion<?>> suggestionReader
        ) {

            super(name, builderReader, builderParser);
            setSuggestionReader(suggestionReader);
        }

        private void setSuggestionReader(Writeable.Reader<? extends Suggest.Suggestion<?>> reader) {
            this.suggestionReader = reader;
        }

        /**
         * Returns the reader used to read the {@link Suggest.Suggestion} generated by this suggester
         */
        @SuppressWarnings("rawtypes")
        public Writeable.Reader<? extends Suggest.Suggestion> getSuggestionReader() {
            return this.suggestionReader;
        }
    }

    /**
     * Specification of custom {@link Query}.
     */
    class QuerySpec<T extends QueryBuilder> extends SearchExtensionSpec<T, QueryParser<T>> {
        /**
         * Specification of custom {@link Query}.
         *
         * @param name holds the names by which this query might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the query should use as its
         *        {@link NamedWriteable#getWriteableName()} too.
         * @param reader the reader registered for this query's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the query builder from xcontent
         */
        public QuerySpec(ParseField name, Writeable.Reader<T> reader, QueryParser<T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link Query}.
         *
         * @param name the name by which this query might be parsed or deserialized. Make sure that the query builder returns this name for
         *        {@link NamedWriteable#getWriteableName()}.
         * @param reader the reader registered for this query's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the query builder from xcontent
         */
        public QuerySpec(String name, Writeable.Reader<T> reader, QueryParser<T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification for an {@link Aggregation}.
     */
    class AggregationSpec extends SearchExtensionSpec<AggregationBuilder, ContextParser<String, ? extends AggregationBuilder>> {
        private final Map<String, Writeable.Reader<? extends InternalAggregation>> resultReaders = new TreeMap<>();
        private Consumer<ValuesSourceRegistry.Builder> aggregatorRegistrar;

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the {@link AggregationBuilder} should return
         *        from {@link NamedWriteable#getWriteableName()}.
         * @param reader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         */
        public <T extends AggregationBuilder> AggregationSpec(
            ParseField name,
            Writeable.Reader<T> reader,
            ContextParser<String, T> parser
        ) {
            super(name, reader, parser);
        }

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name the name by which this aggregation might be parsed or deserialized. Make sure that the {@link AggregationBuilder}
         *        returns this from {@link NamedWriteable#getWriteableName()}.
         * @param reader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         */
        public <T extends AggregationBuilder> AggregationSpec(String name, Writeable.Reader<T> reader, ContextParser<String, T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the {@link AggregationBuilder} should return
         *        from {@link NamedWriteable#getWriteableName()}.
         * @param reader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         * @deprecated Use the ctor that takes a {@link ContextParser} instead
         */
        @Deprecated
        public AggregationSpec(ParseField name, Writeable.Reader<? extends AggregationBuilder> reader, Aggregator.Parser parser) {
            super(name, reader, (p, aggName) -> parser.parse(aggName, p));
        }

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name the name by which this aggregation might be parsed or deserialized. Make sure that the {@link AggregationBuilder}
         *        returns this from {@link NamedWriteable#getWriteableName()}.
         * @param reader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         * @deprecated Use the ctor that takes a {@link ContextParser} instead
         */
        @Deprecated
        public AggregationSpec(String name, Writeable.Reader<? extends AggregationBuilder> reader, Aggregator.Parser parser) {
            super(name, reader, (p, aggName) -> parser.parse(aggName, p));
        }

        /**
         * Add a reader for the shard level results of the aggregation with {@linkplain #getName}'s {@link ParseField#getPreferredName()} as
         * the {@link NamedWriteable#getWriteableName()}.
         */
        public AggregationSpec addResultReader(Writeable.Reader<? extends InternalAggregation> resultReader) {
            return addResultReader(getName().getPreferredName(), resultReader);
        }

        /**
         * Add a reader for the shard level results of the aggregation.
         */
        public AggregationSpec addResultReader(String writeableName, Writeable.Reader<? extends InternalAggregation> resultReader) {
            resultReaders.put(writeableName, resultReader);
            return this;
        }

        /**
         * Get the readers that must be registered for this aggregation's results.
         */
        public Map<String, Writeable.Reader<? extends InternalAggregation>> getResultReaders() {
            return resultReaders;
        }

        /**
         * Get the function to register the {@link org.elasticsearch.search.aggregations.support.ValuesSource} to aggregator mappings for
         * this aggregation
         */
        public Consumer<ValuesSourceRegistry.Builder> getAggregatorRegistrar() {
            return aggregatorRegistrar;
        }

        /**
         * Set the function to register the {@link org.elasticsearch.search.aggregations.support.ValuesSource} to aggregator mappings for
         * this aggregation
         */
        public AggregationSpec setAggregatorRegistrar(Consumer<ValuesSourceRegistry.Builder> aggregatorRegistrar) {
            this.aggregatorRegistrar = aggregatorRegistrar;
            return this;
        }
    }

    /**
     * Specification for a {@link PipelineAggregator}.
     */
    class PipelineAggregationSpec extends SearchExtensionSpec<
        PipelineAggregationBuilder,
        ContextParser<String, ? extends PipelineAggregationBuilder>> {
        private final Map<String, Writeable.Reader<? extends InternalAggregation>> resultReaders = new TreeMap<>();

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the readers are registered. So it is the name that the {@link PipelineAggregationBuilder} and
         *        {@link PipelineAggregator} should return from {@link NamedWriteable#getWriteableName()}.
         * @param builderReader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser reads the aggregation builder from XContent
         */
        public PipelineAggregationSpec(
            ParseField name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            ContextParser<String, ? extends PipelineAggregationBuilder> parser
        ) {
            super(name, builderReader, parser);
        }

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name name by which this aggregation might be parsed or deserialized. Make sure it is the name that the
         *        {@link PipelineAggregationBuilder} and {@link PipelineAggregator} should return from
         *        {@link NamedWriteable#getWriteableName()}.
         * @param builderReader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser reads the aggregation builder from XContent
         */
        public PipelineAggregationSpec(
            String name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            ContextParser<String, ? extends PipelineAggregationBuilder> parser
        ) {
            super(name, builderReader, parser);
        }

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the readers are registered. So it is the name that the {@link PipelineAggregationBuilder} and
         *        {@link PipelineAggregator} should return from {@link NamedWriteable#getWriteableName()}.
         * @param builderReader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser reads the aggregation builder from XContent
         * @deprecated prefer the ctor that takes a {@link ContextParser}
         */
        @Deprecated
        public PipelineAggregationSpec(
            ParseField name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            PipelineAggregator.Parser parser
        ) {
            super(name, builderReader, (p, n) -> parser.parse(n, p));
        }

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name name by which this aggregation might be parsed or deserialized. Make sure it is the name that the
         *        {@link PipelineAggregationBuilder} and {@link PipelineAggregator} should return from
         *        {@link NamedWriteable#getWriteableName()}.
         * @param builderReader the reader registered for this aggregation's builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @deprecated prefer the ctor that takes a {@link ContextParser}
         */
        @Deprecated
        public PipelineAggregationSpec(
            String name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            PipelineAggregator.Parser parser
        ) {
            super(name, builderReader, (p, n) -> parser.parse(n, p));
        }

        /**
         * Add a reader for the shard level results of the aggregation with {@linkplain #getName()}'s {@link ParseField#getPreferredName()}
         * as the {@link NamedWriteable#getWriteableName()}.
         */
        public PipelineAggregationSpec addResultReader(Writeable.Reader<? extends InternalAggregation> resultReader) {
            return addResultReader(getName().getPreferredName(), resultReader);
        }

        /**
         * Add a reader for the shard level results of the aggregation.
         */
        public PipelineAggregationSpec addResultReader(String writeableName, Writeable.Reader<? extends InternalAggregation> resultReader) {
            resultReaders.put(writeableName, resultReader);
            return this;
        }

        /**
         * Get the readers that must be registered for this aggregation's results.
         */
        public Map<String, Writeable.Reader<? extends InternalAggregation>> getResultReaders() {
            return resultReaders;
        }
    }

    /**
     * Specification for a {@link SearchExtBuilder} which represents an additional section that can be
     * parsed in a search request (within the ext element).
     */
    class SearchExtSpec<T extends SearchExtBuilder> extends SearchExtensionSpec<T, CheckedFunction<XContentParser, T, IOException>> {
        public SearchExtSpec(
            ParseField name,
            Writeable.Reader<? extends T> reader,
            CheckedFunction<XContentParser, T, IOException> parser
        ) {
            super(name, reader, parser);
        }

        public SearchExtSpec(String name, Writeable.Reader<? extends T> reader, CheckedFunction<XContentParser, T, IOException> parser) {
            super(name, reader, parser);
        }
    }

    class RescorerSpec<T extends RescorerBuilder<T>> extends SearchExtensionSpec<T, CheckedFunction<XContentParser, T, IOException>> {
        public RescorerSpec(ParseField name, Writeable.Reader<? extends T> reader, CheckedFunction<XContentParser, T, IOException> parser) {
            super(name, reader, parser);
        }

        public RescorerSpec(String name, Writeable.Reader<? extends T> reader, CheckedFunction<XContentParser, T, IOException> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification of search time behavior extension like a custom {@link ScoreFunction}.
     *
     * @param <W> the type of the main {@link NamedWriteable} for this spec. All specs have this but it isn't always *for* the same thing
     *        though, usually it is some sort of builder sent from the coordinating node to the data nodes executing the behavior
     * @param <P> the type of the parser for this spec. The parser runs on the coordinating node, converting {@link XContent} into the
     *        behavior to execute
     */
    class SearchExtensionSpec<W extends NamedWriteable, P> {
        private final ParseField name;
        private final Writeable.Reader<? extends W> reader;
        private final P parser;

        /**
         * Build the spec with a {@linkplain ParseField}.
         *
         * @param name the name of the behavior as a {@linkplain ParseField}. The parser is registered under all names specified by the
         *        {@linkplain ParseField} but the reader is only registered under the {@link ParseField#getPreferredName()} so be sure that
         *        that is the name that W's {@link NamedWriteable#getWriteableName()} returns.
         * @param reader reader that reads the behavior from the internode protocol
         * @param parser parser that read the behavior from a REST request
         */
        public SearchExtensionSpec(ParseField name, Writeable.Reader<? extends W> reader, P parser) {
            this.name = name;
            this.reader = reader;
            this.parser = parser;
        }

        /**
         * Build the spec with a String.
         *
         * @param name the name of the behavior. The parser and the reader are are registered under this name so be sure that that is the
         *        name that W's {@link NamedWriteable#getWriteableName()} returns.
         * @param reader reader that reads the behavior from the internode protocol
         * @param parser parser that read the behavior from a REST request
         */
        public SearchExtensionSpec(String name, Writeable.Reader<? extends W> reader, P parser) {
            this(new ParseField(name), reader, parser);
        }

        /**
         * The name of the thing being specified as a {@link ParseField}. This allows it to have deprecated names.
         */
        public ParseField getName() {
            return name;
        }

        /**
         * The reader responsible for reading the behavior from the internode protocol.
         */
        public Writeable.Reader<? extends W> getReader() {
            return reader;
        }

        /**
         * The parser responsible for converting {@link XContent} into the behavior.
         */
        public P getParser() {
            return parser;
        }
    }

    /**
     * Context available during fetch phase construction.
     */
    class FetchPhaseConstructionContext {
        private final Map<String, Highlighter> highlighters;

        public FetchPhaseConstructionContext(Map<String, Highlighter> highlighters) {
            this.highlighters = highlighters;
        }

        public Map<String, Highlighter> getHighlighters() {
            return highlighters;
        }
    }

    /**
     * Specification of custom {@link QueryVectorBuilder}.
     */
    class QueryVectorBuilderSpec<T extends QueryVectorBuilder> extends SearchExtensionSpec<T, BiFunction<XContentParser, Void, T>> {
        /**
         * Specification of custom {@link QueryVectorBuilder}.
         *
         * @param name holds the names by which this query vector builder might be parsed.
         *             The {@link ParseField#getPreferredName()} is special as it
         *             is the name by under which the reader is registered. So it is the name that the builder should use as its
         *             {@link NamedWriteable#getWriteableName()} too.
         * @param reader the reader registered for this query vector builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the query vector builder from xcontent
         */
        public QueryVectorBuilderSpec(ParseField name, Writeable.Reader<T> reader, BiFunction<XContentParser, Void, T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link QueryVectorBuilder}.
         *
         * @param name the name by which this query vector builder might be parsed or deserialized.
         *             Make sure that the query builder returns this name for {@link NamedWriteable#getWriteableName()}.
         * @param reader the reader registered for this query vector builder. Typically a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the query vector builder from xcontent
         */
        public QueryVectorBuilderSpec(String name, Writeable.Reader<T> reader, BiFunction<XContentParser, Void, T> parser) {
            super(name, reader, parser);
        }
    }
}
