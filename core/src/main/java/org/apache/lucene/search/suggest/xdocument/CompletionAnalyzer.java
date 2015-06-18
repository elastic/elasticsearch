package org.apache.lucene.search.suggest.xdocument;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.util.automaton.Operations;

/**
 * Wraps an {@link org.apache.lucene.analysis.Analyzer}
 * to provide additional completion-only tuning
 * (e.g. preserving token separators, preserving position increments while converting
 * a token stream to an automaton)
 * <p>
 * Can be used to index {@link SuggestField} and {@link ContextSuggestField}
 * and as a query analyzer to {@link PrefixCompletionQuery} amd {@link FuzzyCompletionQuery}
 * <p>
 * NOTE: In most cases, index and query analyzer should have same values for {@link #preservePositionIncrements()}
 * and {@link #preserveSep()}
 *
 * @lucene.experimental
 */
public final class CompletionAnalyzer extends AnalyzerWrapper {

  /**
   * Represents the separation between tokens, if
   * <code>preserveSep</code> is <code>true</code>
   * <p>
   * Same label is used as a delimiter in the {@link CompletionTokenStream}
   * payload
   */
  final static int SEP_LABEL = NRTSuggesterBuilder.PAYLOAD_SEP;

  /**
   * Represent a hole character, inserted by {@link org.apache.lucene.analysis.TokenStreamToAutomaton}
   */
  final static int HOLE_CHARACTER = TokenStreamToAutomaton.HOLE;

  final static int DEFAULT_MAX_GRAPH_EXPANSIONS = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
  final static boolean DEFAULT_PRESERVE_SEP = true;
  final static boolean DEFAULT_PRESERVE_POSITION_INCREMENTS = true;

  private final Analyzer analyzer;

  /**
   * Preserve separation between tokens
   * when converting to an automaton
   * <p>
   * Defaults to <code>true</code>
   */
  private final boolean preserveSep;

  /**
   * Preserve position increments for tokens
   * when converting to an automaton
   * <p>
   * Defaults to <code>true</code>
   */
  private final boolean preservePositionIncrements;

  /**
   * Sets the maximum number of graph expansions of a completion automaton
   * <p>
   * Defaults to <code>-1</code> (no limit)
   */
  private final int maxGraphExpansions;

  /**
   * Wraps an analyzer to convert it's output token stream to an automaton
   *
   * @param analyzer token stream to be converted to an automaton
   * @param preserveSep Preserve separation between tokens when converting to an automaton
   * @param preservePositionIncrements Preserve position increments for tokens when converting to an automaton
   * @param maxGraphExpansions Sets the maximum number of graph expansions of a completion automaton
   */
  public CompletionAnalyzer(Analyzer analyzer, boolean preserveSep, boolean preservePositionIncrements, int maxGraphExpansions) {
    super(PER_FIELD_REUSE_STRATEGY);
    this.analyzer = analyzer;
    this.preserveSep = preserveSep;
    this.preservePositionIncrements = preservePositionIncrements;
    this.maxGraphExpansions = maxGraphExpansions;
  }

  /**
   * Calls {@link #CompletionAnalyzer(org.apache.lucene.analysis.Analyzer, boolean, boolean, int)}
   * preserving token separation, position increments and no limit on graph expansions
   */
  public CompletionAnalyzer(Analyzer analyzer) {
    this(analyzer, DEFAULT_PRESERVE_SEP, DEFAULT_PRESERVE_POSITION_INCREMENTS, DEFAULT_MAX_GRAPH_EXPANSIONS);
  }

  /**
   * Calls {@link #CompletionAnalyzer(org.apache.lucene.analysis.Analyzer, boolean, boolean, int)}
   * with no limit on graph expansions
   */
  public CompletionAnalyzer(Analyzer analyzer, boolean preserveSep, boolean preservePositionIncrements) {
    this(analyzer, preserveSep, preservePositionIncrements, DEFAULT_MAX_GRAPH_EXPANSIONS);
  }

  /**
   * Calls {@link #CompletionAnalyzer(org.apache.lucene.analysis.Analyzer, boolean, boolean, int)}
   * preserving token separation and position increments
   */
  public CompletionAnalyzer(Analyzer analyzer, int maxGraphExpansions) {
    this(analyzer, DEFAULT_PRESERVE_SEP, DEFAULT_PRESERVE_POSITION_INCREMENTS, maxGraphExpansions);
  }

  /**
   * Returns true if separation between tokens are preserved when converting
   * the token stream to an automaton
   */
  public boolean preserveSep() {
    return preserveSep;
  }

  /**
   * Returns true if position increments are preserved when converting
   * the token stream to an automaton
   */
  public boolean preservePositionIncrements() {
    return preservePositionIncrements;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return analyzer;
  }

  @Override
  protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    CompletionTokenStream tokenStream = new CompletionTokenStream(components.getTokenStream(),
        preserveSep, preservePositionIncrements, maxGraphExpansions);
    return new TokenStreamComponents(components.getTokenizer(), tokenStream);
  }
}
