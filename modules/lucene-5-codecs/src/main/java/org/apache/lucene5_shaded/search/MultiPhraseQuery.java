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
import java.util.*;

import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.IndexReaderContext;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.search.similarities.Similarity.SimScorer;
import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.PriorityQueue;
import org.apache.lucene5_shaded.util.ToStringUtils;

/**
 * A generalized version of {@link PhraseQuery}, with an added
 * method {@link #add(Term[])} for adding more than one term at the same position
 * that are treated as a disjunction (OR).
 * To use this class to search for the phrase "Microsoft app*" first use
 * {@link #add(Term)} on the term "microsoft" (assuming lowercase analysis), then
 * find all terms that have "app" as prefix using {@link LeafReader#terms(String)},
 * seeking to "app" then iterating and collecting terms until there is no longer
 * that prefix, and finally use {@link #add(Term[])} to add them to the query.
 */
public class MultiPhraseQuery extends Query {
  private String field;// becomes non-null on first add() then is unmodified
  private final ArrayList<Term[]> termArrays = new ArrayList<>();
  private final ArrayList<Integer> positions = new ArrayList<>();

  private int slop = 0;

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#getSlop()
   */
  public void setSlop(int s) {
    if (s < 0) {
      throw new IllegalArgumentException("slop value cannot be negative");
    }
    slop = s; 
  }

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#getSlop()
   */
  public int getSlop() { return slop; }

  /** Add a single term at the next position in the phrase.
   */
  public void add(Term term) { add(new Term[]{term}); }

  /** Add multiple terms at the next position in the phrase.  Any of the terms
   * may match (a disjunction).
   * The array is not copied or mutated, the caller should consider it
   * immutable subsequent to calling this method.
   */
  public void add(Term[] terms) {
    int position = 0;
    if (positions.size() > 0)
      position = positions.get(positions.size() - 1) + 1;

    add(terms, position);
  }

  /**
   * Allows to specify the relative position of terms within the phrase.
   * The array is not copied or mutated, the caller should consider it
   * immutable subsequent to calling this method.
   */
  public void add(Term[] terms, int position) {
    Objects.requireNonNull(terms, "Term array must not be null");
    if (termArrays.size() == 0)
      field = terms[0].field();

    for (Term term : terms) {
      if (!term.field().equals(field)) {
        throw new IllegalArgumentException(
            "All phrase terms must be in the same field (" + field + "): " + term);
      }
    }

    termArrays.add(terms);
    positions.add(position);
  }

  /**
   * Returns a List of the terms in the multi-phrase.
   * Do not modify the List or its contents.
   */
  public List<Term[]> getTermArrays() {
    return Collections.unmodifiableList(termArrays);
  }

  /**
   * Returns the relative positions of terms in this phrase.
   */
  public int[] getPositions() {
    int[] result = new int[positions.size()];
    for (int i = 0; i < positions.size(); i++)
      result[i] = positions.get(i);
    return result;
  }


  private class MultiPhraseWeight extends Weight {
    private final Similarity similarity;
    private final Similarity.SimWeight stats;
    private final Map<Term,TermContext> termContexts = new HashMap<>();
    private final boolean needsScores;

    public MultiPhraseWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
      super(MultiPhraseQuery.this);
      this.needsScores = needsScores;
      this.similarity = searcher.getSimilarity(needsScores);
      final IndexReaderContext context = searcher.getTopReaderContext();
      
      // compute idf
      ArrayList<TermStatistics> allTermStats = new ArrayList<>();
      for(final Term[] terms: termArrays) {
        for (Term term: terms) {
          TermContext termContext = termContexts.get(term);
          if (termContext == null) {
            termContext = TermContext.build(context, term);
            termContexts.put(term, termContext);
          }
          allTermStats.add(searcher.termStatistics(term, termContext));
        }
      }
      stats = similarity.computeWeight(
          searcher.collectionStatistics(field), 
          allTermStats.toArray(new TermStatistics[allTermStats.size()]));
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (final Term[] arr : termArrays) {
        Collections.addAll(terms, arr);
      }
    }

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
      assert !termArrays.isEmpty();
      final LeafReader reader = context.reader();
      
      PhraseQuery.PostingsAndFreq[] postingsFreqs = new PhraseQuery.PostingsAndFreq[termArrays.size()];

      final Terms fieldTerms = reader.terms(field);
      if (fieldTerms == null) {
        return null;
      }

      // TODO: move this check to createWeight to happen earlier to the user?
      if (fieldTerms.hasPositions() == false) {
        throw new IllegalStateException("field \"" + field + "\" was indexed without position data;" +
            " cannot run MultiPhraseQuery (phrase=" + getQuery() + ")");
      }

      // Reuse single TermsEnum below:
      final TermsEnum termsEnum = fieldTerms.iterator();
      float totalMatchCost = 0;

      for (int pos=0; pos<postingsFreqs.length; pos++) {
        Term[] terms = termArrays.get(pos);
        List<PostingsEnum> postings = new ArrayList<>();
        
        for (Term term : terms) {
          TermState termState = termContexts.get(term).get(context.ord);
          if (termState != null) {
            termsEnum.seekExact(term.bytes(), termState);
            postings.add(termsEnum.postings(null, PostingsEnum.POSITIONS));
            totalMatchCost += PhraseQuery.termPositionsCost(termsEnum);
          }
        }
        
        if (postings.isEmpty()) {
          return null;
        }
        
        final PostingsEnum postingsEnum;
        if (postings.size() == 1) {
          postingsEnum = postings.get(0);
        } else {
          postingsEnum = new UnionPostingsEnum(postings);
        }

        postingsFreqs[pos] = new PhraseQuery.PostingsAndFreq(postingsEnum, positions.get(pos).intValue(), terms);
      }

      // sort by increasing docFreq order
      if (slop == 0) {
        ArrayUtil.timSort(postingsFreqs);
      }

      if (slop == 0) {
        return new ExactPhraseScorer(this, postingsFreqs,
                                      similarity.simScorer(stats, context),
                                      needsScores, totalMatchCost);
      } else {
        return new SloppyPhraseScorer(this, postingsFreqs, slop,
                                        similarity.simScorer(stats, context),
                                        needsScores, totalMatchCost);
      }
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

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    if (termArrays.isEmpty()) {
      return new MatchNoDocsQuery();
    } else if (termArrays.size() == 1) {                 // optimize one-term case
      Term[] terms = termArrays.get(0);
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.setDisableCoord(true);
      for (Term term : terms) {
        builder.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
      }
      return builder.build();
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new MultiPhraseWeight(searcher, needsScores);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public final String toString(String f) {
    StringBuilder buffer = new StringBuilder();
    if (field == null || !field.equals(f)) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    int k = 0;
    Iterator<Term[]> i = termArrays.iterator();
    int lastPos = -1;
    boolean first = true;
    while (i.hasNext()) {
      Term[] terms = i.next();
      int position = positions.get(k);
      if (first) {
        first = false;
      } else {
        buffer.append(" ");
        for (int j=1; j<(position-lastPos); j++) {
          buffer.append("? ");
        }
      }
      if (terms.length > 1) {
        buffer.append("(");
        for (int j = 0; j < terms.length; j++) {
          buffer.append(terms[j].text());
          if (j < terms.length-1)
            buffer.append(" ");
        }
        buffer.append(")");
      } else {
        buffer.append(terms[0].text());
      }
      lastPos = position;
      ++k;
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }

    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MultiPhraseQuery)) return false;
    MultiPhraseQuery other = (MultiPhraseQuery)o;
    return super.equals(o)
      && this.slop == other.slop
      && termArraysEquals(this.termArrays, other.termArrays)
      && this.positions.equals(other.positions);
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return super.hashCode()
      ^ slop
      ^ termArraysHashCode()
      ^ positions.hashCode();
  }
  
  // Breakout calculation of the termArrays hashcode
  private int termArraysHashCode() {
    int hashCode = 1;
    for (final Term[] termArray: termArrays) {
      hashCode = 31 * hashCode
          + (termArray == null ? 0 : Arrays.hashCode(termArray));
    }
    return hashCode;
  }

  // Breakout calculation of the termArrays equals
  private boolean termArraysEquals(List<Term[]> termArrays1, List<Term[]> termArrays2) {
    if (termArrays1.size() != termArrays2.size()) {
      return false;
    }
    ListIterator<Term[]> iterator1 = termArrays1.listIterator();
    ListIterator<Term[]> iterator2 = termArrays2.listIterator();
    while (iterator1.hasNext()) {
      Term[] termArray1 = iterator1.next();
      Term[] termArray2 = iterator2.next();
      if (!(termArray1 == null ? termArray2 == null : Arrays.equals(termArray1,
          termArray2))) {
        return false;
      }
    }
    return true;
  }
  
  /** 
   * Takes the logical union of multiple PostingsEnum iterators.
   * <p>
   * Note: positions are merged during freq()
   */
  static class UnionPostingsEnum extends PostingsEnum {
    /** queue ordered by docid */
    final DocsQueue docsQueue;
    /** cost of this enum: sum of its subs */
    final long cost;
    
    /** queue ordered by position for current doc */
    final PositionsQueue posQueue = new PositionsQueue();
    /** current doc posQueue is working */
    int posQueueDoc = -2;
    /** list of subs (unordered) */
    final PostingsEnum[] subs;
    
    UnionPostingsEnum(Collection<PostingsEnum> subs) {
      docsQueue = new DocsQueue(subs.size());
      long cost = 0;
      for (PostingsEnum sub : subs) {
        docsQueue.add(sub);
        cost += sub.cost();
      }
      this.cost = cost;
      this.subs = subs.toArray(new PostingsEnum[subs.size()]);
    }

    @Override
    public int freq() throws IOException {
      int doc = docID();
      if (doc != posQueueDoc) {
        posQueue.clear();
        for (PostingsEnum sub : subs) {
          if (sub.docID() == doc) {
            int freq = sub.freq();
            for (int i = 0; i < freq; i++) {
              posQueue.add(sub.nextPosition());
            }
          }
        }
        posQueue.sort();
        posQueueDoc = doc;
      }
      return posQueue.size();
    }

    @Override
    public int nextPosition() throws IOException {
      return posQueue.next();
    }

    @Override
    public int docID() {
      return docsQueue.top().docID();
    }

    @Override
    public int nextDoc() throws IOException {
      PostingsEnum top = docsQueue.top();
      int doc = top.docID();
      
      do {
        top.nextDoc();
        top = docsQueue.updateTop();
      } while (top.docID() == doc);

      return top.docID();
    }

    @Override
    public int advance(int target) throws IOException {
      PostingsEnum top = docsQueue.top();
      
      do {
        top.advance(target);
        top = docsQueue.updateTop();
      } while (top.docID() < target);

      return top.docID();
    }

    @Override
    public long cost() {
      return cost;
    }
    
    @Override
    public int startOffset() throws IOException {
      return -1; // offsets are unsupported
    }

    @Override
    public int endOffset() throws IOException {
      return -1; // offsets are unsupported
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null; // payloads are unsupported
    }
    
    /** 
     * disjunction of postings ordered by docid.
     */
    static class DocsQueue extends PriorityQueue<PostingsEnum> {
      DocsQueue(int size) {
        super(size);
      }

      @Override
      public final boolean lessThan(PostingsEnum a, PostingsEnum b) {
        return a.docID() < b.docID();
      }
    }
    
    /** 
     * queue of terms for a single document. its a sorted array of
     * all the positions from all the postings
     */
    static class PositionsQueue {
      private int arraySize = 16;
      private int index = 0;
      private int size = 0;
      private int[] array = new int[arraySize];
      
      void add(int i) {
        if (size == arraySize)
          growArray();

        array[size++] = i;
      }

      int next() {
        return array[index++];
      }

      void sort() {
        Arrays.sort(array, index, size);
      }

      void clear() {
        index = 0;
        size = 0;
      }

      int size() {
        return size;
      }

      private void growArray() {
        int[] newArray = new int[arraySize * 2];
        System.arraycopy(array, 0, newArray, 0, arraySize);
        array = newArray;
        arraySize *= 2;
      }
    }
  }
}
