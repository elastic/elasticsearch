/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.suggest.Suggester;

import java.util.List;
import java.util.Map;

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
    default List<SearchExtensionSpec<SignificanceHeuristic, SignificanceHeuristicParser>> getSignificanceHeuristics() {
        return emptyList();
    }
    /**
     * The new {@link MovAvgModel}s defined by this plugin. {@linkplain MovAvgModel}s are used by the {@link MovAvgPipelineAggregator} to
     * model trends in data.
     */
    default List<SearchExtensionSpec<MovAvgModel, MovAvgModel.AbstractModelParser>> getMovingAverageModels() {
        return emptyList();
    }
    /**
     * The new {@link FetchSubPhase}s defined by this plugin.
     */
    default List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
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
    default Map<String, Suggester<?>> getSuggesters() {
        return emptyMap();
    }
    /**
     * The new {@link Query}s defined by this plugin.
     */
    default List<QuerySpec<?>> getQueries() {
        return emptyList();
    }

    /**
     * Specification of custom {@link ScoreFunction}.
     */
    class ScoreFunctionSpec<T extends ScoreFunctionBuilder<T>> extends SearchExtensionSpec<T, ScoreFunctionParser<T>> {
        public ScoreFunctionSpec(ParseField name, Reader<T> reader, ScoreFunctionParser<T> parser) {
            super(name, reader, parser);
        }

        public ScoreFunctionSpec(String name, Reader<T> reader, ScoreFunctionParser<T> parser) {
            super(name, reader, parser);
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
        public QuerySpec(ParseField name, Reader<T> reader, QueryParser<T> parser) {
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
        public QuerySpec(String name, Reader<T> reader, QueryParser<T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification of search time behavior extension like a custom {@link MovAvgModel} or {@link ScoreFunction}.
     *
     * @param W the type of the main {@link NamedWriteable} for this spec. All specs have this but it isn't always *for* the same thing
     *        though, usually it is some sort of builder sent from the coordinating node to the data nodes executing the behavior
     * @param P the type of the parser for this spec. The parser runs on the coordinating node, converting {@link XContent} into the
     *        behavior to execute
     */
    class SearchExtensionSpec<W extends NamedWriteable, P> {
        private final ParseField name;
        private final Writeable.Reader<W> reader;
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
        public SearchExtensionSpec(ParseField name, Writeable.Reader<W> reader, P parser) {
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
        public SearchExtensionSpec(String name, Writeable.Reader<W> reader, P parser) {
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
        public Writeable.Reader<W> getReader() {
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
}
