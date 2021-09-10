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
package org.apache.lucene5_shaded.util;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene5_shaded.analysis.Analyzer;
import org.apache.lucene5_shaded.analysis.CachingTokenFilter;
import org.apache.lucene5_shaded.analysis.TokenStream;
import org.apache.lucene5_shaded.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene5_shaded.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.search.BooleanClause;
import org.apache.lucene5_shaded.search.BooleanQuery;
import org.apache.lucene5_shaded.search.MultiPhraseQuery;
import org.apache.lucene5_shaded.search.PhraseQuery;
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.search.TermQuery;

/**
 * Creates queries from the {@link Analyzer} chain.
 * <p>
 * Example usage:
 * <pre class="prettyprint">
 *   QueryBuilder builder = new QueryBuilder(analyzer);
 *   Query a = builder.createBooleanQuery("body", "just a test");
 *   Query b = builder.createPhraseQuery("body", "another test");
 *   Query c = builder.createMinShouldMatchQuery("body", "another test", 0.5f);
 * </pre>
 * <p>
 * This can also be used as a subclass for query parsers to make it easier
 * to interact with the analysis chain. Factory methods such as {@code newTermQuery} 
 * are provided so that the generated queries can be customized.
 */
public class QueryBuilder {
  private Analyzer analyzer;
  private boolean enablePositionIncrements = true;
  
  /** Creates a new QueryBuilder using the given analyzer. */
  public QueryBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
  
  /** 
   * Creates a boolean query from the query text.
   * <p>
   * This is equivalent to {@code createBooleanQuery(field, queryText, Occur.SHOULD)}
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis
   *         of {@code queryText}
   */
  public Query createBooleanQuery(String field, String queryText) {
    return createBooleanQuery(field, queryText, BooleanClause.Occur.SHOULD);
  }
  
  /** 
   * Creates a boolean query from the query text.
   * <p>
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @param operator operator used for clauses between analyzer tokens.
   * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis 
   *         of {@code queryText}
   */
  public Query createBooleanQuery(String field, String queryText, BooleanClause.Occur operator) {
    if (operator != BooleanClause.Occur.SHOULD && operator != BooleanClause.Occur.MUST) {
      throw new IllegalArgumentException("invalid operator: only SHOULD or MUST are allowed");
    }
    return createFieldQuery(analyzer, operator, field, queryText, false, 0);
  }
  
  /** 
   * Creates a phrase query from the query text.
   * <p>
   * This is equivalent to {@code createPhraseQuery(field, queryText, 0)}
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or
   *         {@code MultiPhraseQuery}, based on the analysis of {@code queryText}
   */
  public Query createPhraseQuery(String field, String queryText) {
    return createPhraseQuery(field, queryText, 0);
  }
  
  /** 
   * Creates a phrase query from the query text.
   * <p>
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @param phraseSlop number of other words permitted between words in query phrase
   * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or
   *         {@code MultiPhraseQuery}, based on the analysis of {@code queryText}
   */
  public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
    return createFieldQuery(analyzer, BooleanClause.Occur.MUST, field, queryText, true, phraseSlop);
  }
  
  /** 
   * Creates a minimum-should-match query from the query text.
   * <p>
   * @param field field name
   * @param queryText text to be passed to the analyzer
   * @param fraction of query terms {@code [0..1]} that should match 
   * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis 
   *         of {@code queryText}
   */
  public Query createMinShouldMatchQuery(String field, String queryText, float fraction) {
    if (Float.isNaN(fraction) || fraction < 0 || fraction > 1) {
      throw new IllegalArgumentException("fraction should be >= 0 and <= 1");
    }
    
    // TODO: wierd that BQ equals/rewrite/scorer doesn't handle this?
    if (fraction == 1) {
      return createBooleanQuery(field, queryText, BooleanClause.Occur.MUST);
    }
    
    Query query = createFieldQuery(analyzer, BooleanClause.Occur.SHOULD, field, queryText, false, 0);
    if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) query;
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.setDisableCoord(bq.isCoordDisabled());
      builder.setMinimumNumberShouldMatch((int) (fraction * bq.clauses().size()));
      for (BooleanClause clause : bq) {
        builder.add(clause);
      }
      query = builder.build();
    }
    return query;
  }
  
  /** 
   * Returns the analyzer. 
   * @see #setAnalyzer(Analyzer)
   */
  public Analyzer getAnalyzer() {
    return analyzer;
  }
  
  /** 
   * Sets the analyzer used to tokenize text.
   */
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
  
  /**
   * Returns true if position increments are enabled.
   * @see #setEnablePositionIncrements(boolean)
   */
  public boolean getEnablePositionIncrements() {
    return enablePositionIncrements;
  }
  
  /**
   * Set to <code>true</code> to enable position increments in result query.
   * <p>
   * When set, result phrase and multi-phrase queries will
   * be aware of position increments.
   * Useful when e.g. a StopFilter increases the position increment of
   * the token that follows an omitted token.
   * <p>
   * Default: true.
   */
  public void setEnablePositionIncrements(boolean enable) {
    this.enablePositionIncrements = enable;
  }

  /**
   * Creates a query from the analysis chain.
   * <p>
   * Expert: this is more useful for subclasses such as queryparsers. 
   * If using this class directly, just use {@link #createBooleanQuery(String, String)}
   * and {@link #createPhraseQuery(String, String)}
   * @param analyzer analyzer used for this query
   * @param operator default boolean operator used for this query
   * @param field field to create queries against
   * @param queryText text to be passed to the analysis chain
   * @param quoted true if phrases should be generated when terms occur at more than one position
   * @param phraseSlop slop factor for phrase/multiphrase queries
   */
  protected final Query createFieldQuery(Analyzer analyzer, BooleanClause.Occur operator, String field, String queryText, boolean quoted, int phraseSlop) {
    assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;
    
    // Use the analyzer to get all the tokens, and then build an appropriate
    // query based on the analysis chain.
    
    try (TokenStream source = analyzer.tokenStream(field, queryText);
         CachingTokenFilter stream = new CachingTokenFilter(source)) {
      
      TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
      PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
      
      if (termAtt == null) {
        return null; 
      }
      
      // phase 1: read through the stream and assess the situation:
      // counting the number of tokens/positions and marking if we have any synonyms.
      
      int numTokens = 0;
      int positionCount = 0;
      boolean hasSynonyms = false;

      stream.reset();
      while (stream.incrementToken()) {
        numTokens++;
        int positionIncrement = posIncAtt.getPositionIncrement();
        if (positionIncrement != 0) {
          positionCount += positionIncrement;
        } else {
          hasSynonyms = true;
        }
      }
      
      // phase 2: based on token count, presence of synonyms, and options
      // formulate a single term, boolean, or phrase.
      
      if (numTokens == 0) {
        return null;
      } else if (numTokens == 1) {
        // single term
        return analyzeTerm(field, stream);
      } else if (quoted && positionCount > 1) {
        // phrase
        if (hasSynonyms) {
          // complex phrase with synonyms
          return analyzeMultiPhrase(field, stream, phraseSlop);
        } else {
          // simple phrase
          return analyzePhrase(field, stream, phraseSlop);
        }
      } else {
        // boolean
        if (positionCount == 1) {
          // only one position, with synonyms
          return analyzeBoolean(field, stream);
        } else {
          // complex case: multiple positions
          return analyzeMultiBoolean(field, stream, operator);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error analyzing query text", e);
    }
  }
  
  /** 
   * Creates simple term query from the cached tokenstream contents 
   */
  private Query analyzeTerm(String field, TokenStream stream) throws IOException {
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    
    stream.reset();
    if (!stream.incrementToken()) {
      throw new AssertionError();
    }
    
    return newTermQuery(new Term(field, termAtt.getBytesRef()));
  }
  
  /** 
   * Creates simple boolean query from the cached tokenstream contents 
   */
  private Query analyzeBoolean(String field, TokenStream stream) throws IOException {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.setDisableCoord(true);

    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    
    stream.reset();
    while (stream.incrementToken()) {
      Query currentQuery = newTermQuery(new Term(field, termAtt.getBytesRef()));
      q.add(currentQuery, BooleanClause.Occur.SHOULD);
    }
    
    return q.build();
  }

  private void add(BooleanQuery.Builder q, BooleanQuery current, BooleanClause.Occur operator) {
    if (current.clauses().isEmpty()) {
      return;
    }
    if (current.clauses().size() == 1) {
      q.add(current.clauses().iterator().next().getQuery(), operator);
    } else {
      q.add(current, operator);
    }
  }

  /** 
   * Creates complex boolean query from the cached tokenstream contents 
   */
  private Query analyzeMultiBoolean(String field, TokenStream stream, BooleanClause.Occur operator) throws IOException {
    BooleanQuery.Builder q = newBooleanQuery(false);
    BooleanQuery.Builder currentQuery = newBooleanQuery(true);
    
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    
    stream.reset();
    while (stream.incrementToken()) {
      BytesRef bytes = termAtt.getBytesRef();
      if (posIncrAtt.getPositionIncrement() != 0) {
        add(q, currentQuery.build(), operator);
        currentQuery = newBooleanQuery(true);
      }
      currentQuery.add(newTermQuery(new Term(field, termAtt.getBytesRef())), BooleanClause.Occur.SHOULD);
    }
    add(q, currentQuery.build(), operator);
    
    return q.build();
  }
  
  /** 
   * Creates simple phrase query from the cached tokenstream contents 
   */
  private Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.setSlop(slop);
    
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    int position = -1;    
    
    stream.reset();
    while (stream.incrementToken()) {
      BytesRef bytes = termAtt.getBytesRef();
      if (enablePositionIncrements) {
        position += posIncrAtt.getPositionIncrement();
      } else {
        position += 1;
      }
      builder.add(new Term(field, bytes), position);
    }

    return builder.build();
  }
  
  /** 
   * Creates complex phrase query from the cached tokenstream contents 
   */
  private Query analyzeMultiPhrase(String field, TokenStream stream, int slop) throws IOException {
    MultiPhraseQuery mpq = newMultiPhraseQuery();
    mpq.setSlop(slop);
    
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    int position = -1;  
    
    List<Term> multiTerms = new ArrayList<>();
    stream.reset();
    while (stream.incrementToken()) {
      int positionIncrement = posIncrAtt.getPositionIncrement();
      
      if (positionIncrement > 0 && multiTerms.size() > 0) {
        if (enablePositionIncrements) {
          mpq.add(multiTerms.toArray(new Term[0]), position);
        } else {
          mpq.add(multiTerms.toArray(new Term[0]));
        }
        multiTerms.clear();
      }
      position += positionIncrement;
      multiTerms.add(new Term(field, termAtt.getBytesRef()));
    }
    
    if (enablePositionIncrements) {
      mpq.add(multiTerms.toArray(new Term[0]), position);
    } else {
      mpq.add(multiTerms.toArray(new Term[0]));
    }
    return mpq;
  }
  
  /**
   * Builds a new BooleanQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @param disableCoord disable coord
   * @return new BooleanQuery instance
   */
  protected BooleanQuery.Builder newBooleanQuery(boolean disableCoord) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.setDisableCoord(disableCoord);
    return builder;
  }
  
  /**
   * Builds a new TermQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @param term term
   * @return new TermQuery instance
   */
  protected Query newTermQuery(Term term) {
    return new TermQuery(term);
  }
  
  /**
   * Builds a new MultiPhraseQuery instance.
   * <p>
   * This is intended for subclasses that wish to customize the generated queries.
   * @return new MultiPhraseQuery instance
   */
  protected MultiPhraseQuery newMultiPhraseQuery() {
    return new MultiPhraseQuery();
  }
}
