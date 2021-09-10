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
package org.apache.lucene5_shaded.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsReader;
import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.IndexReaderContext;
import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.search.similarities.Similarity.SimScorer;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.ToStringUtils;

/** A Query that matches documents containing a particular sequence of terms.
 * A PhraseQuery is built by QueryParser for input like <code>"new york"</code>.
 * 
 * <p>This query may be combined with other terms or queries with a {@link BooleanQuery}.
 *
 * <p><b>NOTE</b>:
 * All terms in the phrase must match, even those at the same position. If you
 * have terms at the same position, perhaps synonyms, you probably want {@link MultiPhraseQuery}
 * instead which only requires one term at a position to match.
 * <br >Also, Leading holes don't have any particular meaning for this query
 * and will be ignored. For instance this query:
 * <pre class="prettyprint">
 * PhraseQuery.Builder builder = new PhraseQuery.Builder();
 * builder.add(new Term("body", "one"), 4);
 * builder.add(new Term("body", "two"), 5);
 * PhraseQuery pq = builder.build();
 * </pre>
 * is equivalent to the below query:
 * <pre class="prettyprint">
 * PhraseQuery.Builder builder = new PhraseQuery.Builder();
 * builder.add(new Term("body", "one"), 0);
 * builder.add(new Term("body", "two"), 1);
 * PhraseQuery pq = builder.build();
 * </pre>
 */
public class PhraseQuery extends Query {

  /** A builder for phrase queries. */
  public static class Builder {

    private int slop;
    private final List<Term> terms;
    private final List<Integer> positions;

    /** Sole constructor. */
    public Builder() {
      slop = 0;
      terms = new ArrayList<>();
      positions = new ArrayList<>();
    }

    /**
     * Set the slop.
     * @see PhraseQuery#getSlop()
     */
    public Builder setSlop(int slop) {
      this.slop = slop;
      return this;
    }

    /**
     * Adds a term to the end of the query phrase.
     * The relative position of the term is the one immediately after the last term added.
     */
    public Builder add(Term term) {
      return add(term, positions.isEmpty() ? 0 : 1 + positions.get(positions.size() - 1));
    }

    /**
     * Adds a term to the end of the query phrase.
     * The relative position of the term within the phrase is specified explicitly, but must be greater than
     * or equal to that of the previously added term.
     * A greater position allows phrases with gaps (e.g. in connection with stopwords).
     * If the position is equal, you most likely should be using
     * {@link MultiPhraseQuery} instead which only requires one term at each position to match; this class requires
     * all of them.
     */
    public Builder add(Term term, int position) {
      if (position < 0) {
        throw new IllegalArgumentException("Positions must be >= 0, got " + position);
      }
      if (positions.isEmpty() == false) {
        final int lastPosition = positions.get(positions.size() - 1);
        if (position < lastPosition) {
          throw new IllegalArgumentException("Positions must be added in order, got " + position + " after " + lastPosition);
        }
      }
      if (terms.isEmpty() == false && term.field().equals(terms.get(0).field()) == false) {
        throw new IllegalArgumentException("All terms must be on the same field, got " + term.field() + " and " + terms.get(0).field());
      }
      terms.add(term);
      positions.add(position);
      return this;
    }

    /**
     * Build a phrase query based on the terms that have been added.
     */
    public PhraseQuery build() {
      Term[] terms = this.terms.toArray(new Term[this.terms.size()]);
      int[] positions = new int[this.positions.size()];
      for (int i = 0; i < positions.length; ++i) {
        positions[i] = this.positions.get(i);
      }
      return new PhraseQuery(slop, terms, positions);
    }

  }

  private final boolean mutable;
  private int slop;
  private String field;
  private final List<Term> terms;
  private final List<Integer> positions;

  private PhraseQuery(int slop, Term[] terms, int[] positions) {
    if (terms.length != positions.length) {
      throw new IllegalArgumentException("Must have as many terms as positions");
    }
    if (slop < 0) {
      throw new IllegalArgumentException("Slop must be >= 0, got " + slop);
    }
    for (int i = 1; i < terms.length; ++i) {
      if (terms[i-1].field().equals(terms[i].field()) == false) {
        throw new IllegalArgumentException("All terms should have the same field");
      }
    }
    for (int position : positions) {
      if (position < 0) {
        throw new IllegalArgumentException("Positions must be >= 0, got " + position);
      }
    }
    for (int i = 1; i < positions.length; ++i) {
      if (positions[i] < positions[i - 1]) {
        throw new IllegalArgumentException("Positions should not go backwards, got "
            + positions[i-1] + " before " + positions[i]);
      }
    }
    this.slop = slop;
    this.terms = Arrays.asList(terms);
    this.positions = new ArrayList<>(positions.length);
    for (int pos : positions) {
      this.positions.add(pos);
    }
    this.field = terms.length == 0 ? null : terms[0].field();
    this.mutable = false;
  }

  private static int[] incrementalPositions(int length) {
    int[] positions = new int[length];
    for (int i = 0; i < length; ++i) {
      positions[i] = i;
    }
    return positions;
  }

  private static Term[] toTerms(String field, String... termStrings) {
    Term[] terms = new Term[termStrings.length];
    for (int i = 0; i < terms.length; ++i) {
      terms[i] = new Term(field, termStrings[i]);
    }
    return terms;
  }

  private static Term[] toTerms(String field, BytesRef... termBytes) {
    Term[] terms = new Term[termBytes.length];
    for (int i = 0; i < terms.length; ++i) {
      terms[i] = new Term(field, termBytes[i]);
    }
    return terms;
  }

  /**
   * Create a phrase query which will match documents that contain the given
   * list of terms at consecutive positions in {@code field}, and at a
   * maximum edit distance of {@code slop}. For more complicated use-cases,
   * use {@link Builder}.
   * @see #getSlop()
   */
  public PhraseQuery(int slop, String field, String... terms) {
    this(slop, toTerms(field, terms), incrementalPositions(terms.length));
  }

  /**
   * Create a phrase query which will match documents that contain the given
   * list of terms at consecutive positions in {@code field}.
   */
  public PhraseQuery(String field, String... terms) {
    this(0, field, terms);
  }

  /**
   * Create a phrase query which will match documents that contain the given
   * list of terms at consecutive positions in {@code field}, and at a
   * maximum edit distance of {@code slop}. For more complicated use-cases,
   * use {@link Builder}.
   * @see #getSlop()
   */
  public PhraseQuery(int slop, String field, BytesRef... terms) {
    this(slop, toTerms(field, terms), incrementalPositions(terms.length));
  }

  /**
   * Create a phrase query which will match documents that contain the given
   * list of terms at consecutive positions in {@code field}.
   */
  public PhraseQuery(String field, BytesRef... terms) {
    this(0, field, terms);
  }

  /**
   * Return the slop for this {@link PhraseQuery}.
   *
   * <p>The slop is an edit distance between respective positions of terms as
   * defined in this {@link PhraseQuery} and the positions of terms in a
   * document.
   *
   * <p>For instance, when searching for {@code "quick fox"}, it is expected that
   * the difference between the positions of {@code fox} and {@code quick} is 1.
   * So {@code "a quick brown fox"} would be at an edit distance of 1 since the
   * difference of the positions of {@code fox} and {@code quick} is 2.
   * Similarly, {@code "the fox is quick"} would be at an edit distance of 3
   * since the difference of the positions of {@code fox} and {@code quick} is -2.
   * The slop defines the maximum edit distance for a document to match.
   *
   * <p>More exact matches are scored higher than sloppier matches, thus search
   * results are sorted by exactness.
   */
  public int getSlop() { return slop; }

  /** Returns the list of terms in this phrase. */
  public Term[] getTerms() {
    return terms.toArray(new Term[0]);
  }

  /**
   * Returns the relative positions of terms in this phrase.
   */
  public int[] getPositions() {
    int[] result = new int[positions.size()];
    for(int i = 0; i < positions.size(); i++)
        result[i] = positions.get(i).intValue();
    return result;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (terms.isEmpty()) {
      Query rewritten = new MatchNoDocsQuery();
      rewritten.setBoost(getBoost());
      return rewritten;
    } else if (terms.size() == 1) {
      TermQuery tq = new TermQuery(terms.get(0));
      tq.setBoost(getBoost());
      return tq;
    } else if (positions.get(0).intValue() != 0) {
      final int[] oldPositions = getPositions();
      int[] newPositions = new int[oldPositions.length];
      for (int i = 0; i < oldPositions.length; ++i) {
        newPositions[i] = oldPositions[i] - oldPositions[0];
      }
      PhraseQuery rewritten = new PhraseQuery(slop, getTerms(), newPositions);
      rewritten.setBoost(getBoost());
      return rewritten;
    } else {
      return super.rewrite(reader);
    }
  }

  static class PostingsAndFreq implements Comparable<PostingsAndFreq> {
    final PostingsEnum postings;
    final int position;
    final Term[] terms;
    final int nTerms; // for faster comparisons

    public PostingsAndFreq(PostingsEnum postings, int position, Term... terms) {
      this.postings = postings;
      this.position = position;
      nTerms = terms==null ? 0 : terms.length;
      if (nTerms>0) {
        if (terms.length==1) {
          this.terms = terms;
        } else {
          Term[] terms2 = new Term[terms.length];
          System.arraycopy(terms, 0, terms2, 0, terms.length);
          Arrays.sort(terms2);
          this.terms = terms2;
        }
      } else {
        this.terms = null;
      }
    }

    @Override
    public int compareTo(PostingsAndFreq other) {
      if (position != other.position) {
        return position - other.position;
      }
      if (nTerms != other.nTerms) {
        return nTerms - other.nTerms;
      }
      if (nTerms == 0) {
        return 0;
      }
      for (int i=0; i<terms.length; i++) {
        int res = terms[i].compareTo(other.terms[i]);
        if (res!=0) return res;
      }
      return 0;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + position;
      for (int i=0; i<nTerms; i++) {
        result = prime * result + terms[i].hashCode(); 
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      PostingsAndFreq other = (PostingsAndFreq) obj;
      if (position != other.position) return false;
      if (terms == null) return other.terms == null;
      return Arrays.equals(terms, other.terms);
    }
  }

  private class PhraseWeight extends Weight {
    private final Similarity similarity;
    private final Similarity.SimWeight stats;
    private final boolean needsScores;
    private transient TermContext states[];
    private final Term[] terms = getTerms();
    private final int[] positions = getPositions();

    public PhraseWeight(IndexSearcher searcher, boolean needsScores)
        throws IOException {
      super(PhraseQuery.this);
      final int[] positions = PhraseQuery.this.getPositions();
      if (positions.length < 2) {
        throw new IllegalStateException("PhraseWeight does not support less than 2 terms, call rewrite first");
      } else if (positions[0] != 0) {
        throw new IllegalStateException("PhraseWeight requires that the first position is 0, call rewrite first");
      }
      this.needsScores = needsScores;
      this.similarity = searcher.getSimilarity(needsScores);
      final IndexReaderContext context = searcher.getTopReaderContext();
      states = new TermContext[terms.length];
      TermStatistics termStats[] = new TermStatistics[terms.length];
      for (int i = 0; i < terms.length; i++) {
        final Term term = terms[i];
        states[i] = TermContext.build(context, term);
        termStats[i] = searcher.termStatistics(term, states[i]);
      }
      stats = similarity.computeWeight(searcher.collectionStatistics(field), termStats);
    }

    @Override
    public void extractTerms(Set<Term> queryTerms) {
      Collections.addAll(queryTerms, terms);
    }

    @Override
    public String toString() { return "weight(" + PhraseQuery.this + ")"; }

    @Override
    public float getValueForNormalization() {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float boost) {
      stats.normalize(queryNorm, boost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      assert terms.length > 0;
      final LeafReader reader = context.reader();
      PostingsAndFreq[] postingsFreqs = new PostingsAndFreq[terms.length];

      final Terms fieldTerms = reader.terms(field);
      if (fieldTerms == null) {
        return null;
      }

      if (fieldTerms.hasPositions() == false) {
        throw new IllegalStateException("field \"" + field + "\" was indexed without position data; cannot run PhraseQuery (phrase=" + getQuery() + ")");
      }

      // Reuse single TermsEnum below:
      final TermsEnum te = fieldTerms.iterator();
      float totalMatchCost = 0;
      
      for (int i = 0; i < terms.length; i++) {
        final Term t = terms[i];
        final TermState state = states[i].get(context.ord);
        if (state == null) { /* term doesnt exist in this segment */
          assert termNotInReader(reader, t): "no termstate found but term exists in reader";
          return null;
        }
        te.seekExact(t.bytes(), state);
        PostingsEnum postingsEnum = te.postings(null, PostingsEnum.POSITIONS);
        postingsFreqs[i] = new PostingsAndFreq(postingsEnum, positions[i], t);
        totalMatchCost += termPositionsCost(te);
      }

      // sort by increasing docFreq order
      if (slop == 0) {
        ArrayUtil.timSort(postingsFreqs);
      }

      if (slop == 0) {  // optimize exact case
        return new ExactPhraseScorer(this, postingsFreqs,
                                      similarity.simScorer(stats, context),
                                      needsScores, totalMatchCost);
      } else {
        return new SloppyPhraseScorer(this, postingsFreqs, slop,
                                        similarity.simScorer(stats, context),
                                        needsScores, totalMatchCost);
      }
    }
    
    // only called from assert
    private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
      return reader.docFreq(term) == 0;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          float freq = slop == 0 ? scorer.freq() : ((SloppyPhraseScorer)scorer).sloppyFreq();
          SimScorer docScorer = similarity.simScorer(stats, context);
          Explanation freqExplanation = Explanation.match(freq, "phraseFreq=" + freq);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
        }
      }
      
      return Explanation.noMatch("no matching term");
    }
  }

  /** A guess of
   * the average number of simple operations for the initial seek and buffer refill
   * per document for the positions of a term.
   * See also {@link Lucene50PostingsReader.BlockPostingsEnum#nextPosition()}.
   * <p>
   * Aside: Instead of being constant this could depend among others on
   * {@link Lucene50PostingsFormat#BLOCK_SIZE},
   * {@link TermsEnum#docFreq()},
   * {@link TermsEnum#totalTermFreq()},
   * {@link DocIdSetIterator#cost()} (expected number of matching docs),
   * {@link LeafReader#maxDoc()} (total number of docs in the segment),
   * and the seek time and block size of the device storing the index.
   */
  private static final int TERM_POSNS_SEEK_OPS_PER_DOC = 128;

  /** Number of simple operations in {@link Lucene50PostingsReader.BlockPostingsEnum#nextPosition()}
   *  when no seek or buffer refill is done.
   */
  private static final int TERM_OPS_PER_POS = 7;

  /** Returns an expected cost in simple operations
   *  of processing the occurrences of a term
   *  in a document that contains the term.
   *  This is for use by {@link TwoPhaseIterator#matchCost} implementations.
   *  <br>This may be inaccurate when {@link TermsEnum#totalTermFreq()} is not available.
   *  @param termsEnum The term is the term at which this TermsEnum is positioned.
   */
  static float termPositionsCost(TermsEnum termsEnum) throws IOException {
    int docFreq = termsEnum.docFreq();
    assert docFreq > 0;
    long totalTermFreq = termsEnum.totalTermFreq(); // -1 when not available
    float expOccurrencesInMatchingDoc = (totalTermFreq < docFreq) ? 1 : (totalTermFreq / (float) docFreq);
    return TERM_POSNS_SEEK_OPS_PER_DOC + expOccurrencesInMatchingDoc * TERM_OPS_PER_POS;
  }


  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new PhraseWeight(searcher, needsScores);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String f) {
    final Term[] terms = getTerms();
    final int[] positions = getPositions();

    StringBuilder buffer = new StringBuilder();
    if (field != null && !field.equals(f)) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    final int maxPosition;
    if (positions.length == 0) {
      maxPosition = -1;
    } else {
      maxPosition = positions[positions.length - 1];
    }
    String[] pieces = new String[maxPosition + 1];
    for (int i = 0; i < terms.length; i++) {
      int pos = positions[i];
      String s = pieces[pos];
      if (s == null) {
        s = (terms[i]).text();
      } else {
        s = s + "|" + (terms[i]).text();
      }
      pieces[pos] = s;
    }
    for (int i = 0; i < pieces.length; i++) {
      if (i > 0) {
        buffer.append(' ');
      }
      String s = pieces[i];
      if (s == null) {
        buffer.append('?');
      } else {
        buffer.append(s);
      }
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }
    buffer.append(ToStringUtils.boost(getBoost()));

    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    PhraseQuery that = (PhraseQuery) o;
    return slop == that.slop
        && terms.equals(that.terms)
        && positions.equals(that.positions);
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + slop;
    h = 31 * h + terms.hashCode();
    h = 31 * h + positions.hashCode();
    return h;
  }

  // Backward compatibility for pre-5.3 PhraseQuery APIs

  /** Constructs an empty phrase query.
   * @deprecated Use the {@link Builder} class to build phrase queries.
   */
  @Deprecated
  public PhraseQuery() {
    this.terms = new ArrayList<>();
    this.positions = new ArrayList<>();
    this.mutable = true;
  }

  private void ensureMutable(String method) {
    if (mutable == false) {
      throw new IllegalStateException("This PhraseQuery has been created with the new "
          + "PhraseQuery.Builder API. It must not be modified afterwards. The "
          + method + " method only exists for backward compatibility");
    }
  }

  /**
   * Set the slop.
   * @see #getSlop()
   * @deprecated Phrase queries should be created once with {@link Builder}
   *             and then considered immutable. See {@link Builder#setSlop(int)}.
   */
  @Deprecated
  public void setSlop(int s) {
    ensureMutable("setSlop");
    if (s < 0) {
      throw new IllegalArgumentException("slop value cannot be negative");
    }
    slop = s; 
  }

  /**
   * Adds a term to the end of the query phrase.
   * The relative position of the term is the one immediately after the last term added.
   * @deprecated Phrase queries should be created once with {@link Builder}
   *             and then considered immutable. See {@link Builder#add(Term)}.
   */
  @Deprecated
  public void add(Term term) {
    int position = 0;
    if (positions.size() > 0) {
      position = positions.get(positions.size()-1) + 1;
    }

    add(term, position);
  }

  /**
   * Adds a term to the end of the query phrase.
   * The relative position of the term within the phrase is specified explicitly.
   * This allows e.g. phrases with more than one term at the same position
   * or phrases with gaps (e.g. in connection with stopwords).
   * @deprecated Phrase queries should be created once with {@link Builder}
   *             and then considered immutable. See {@link Builder#add(Term, int)}.
   */
  @Deprecated
  public void add(Term term, int position) {
    ensureMutable("add");
    Objects.requireNonNull(term, "Term must not be null");
    if (positions.size() > 0) {
      final int previousPosition = positions.get(positions.size()-1);
      if (position < previousPosition) {
        throw new IllegalArgumentException("Positions must be added in order. Got position="
            + position + " while previous position was " + previousPosition);
      }
    } else if (position < 0) {
      throw new IllegalArgumentException("Positions must be positive, got " + position);
    }

    if (terms.size() == 0) {
      field = term.field();
    } else if (!term.field().equals(field)) {
      throw new IllegalArgumentException("All phrase terms must be in the same field: " + term);
    }

    terms.add(term);
    positions.add(Integer.valueOf(position));
  }
}
