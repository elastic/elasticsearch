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
import java.util.Comparator;
import java.util.List;

import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.FilteredTermsEnum;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.Attribute;
import org.apache.lucene5_shaded.util.AttributeImpl;
import org.apache.lucene5_shaded.util.AttributeReflector;
import org.apache.lucene5_shaded.util.AttributeSource;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;
import org.apache.lucene5_shaded.util.UnicodeUtil;
import org.apache.lucene5_shaded.util.automaton.Automaton;
import org.apache.lucene5_shaded.util.automaton.ByteRunAutomaton;
import org.apache.lucene5_shaded.util.automaton.CompiledAutomaton;
import org.apache.lucene5_shaded.util.automaton.LevenshteinAutomata;

/** Subclass of TermsEnum for enumerating all terms that are similar
 * to the specified filter term.
 *
 * <p>Term enumerations are always ordered by
 * {@link BytesRef#compareTo}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 */
public class FuzzyTermsEnum extends TermsEnum {
  private TermsEnum actualEnum;
  private BoostAttribute actualBoostAtt;
  
  private final BoostAttribute boostAtt =
    attributes().addAttribute(BoostAttribute.class);
  
  private final MaxNonCompetitiveBoostAttribute maxBoostAtt;
  private final LevenshteinAutomataAttribute dfaAtt;
  
  private float bottom;
  private BytesRef bottomTerm;

  // TODO: chicken-and-egg
  private final Comparator<BytesRef> termComparator = BytesRef.getUTF8SortedAsUnicodeComparator();
  
  protected final float minSimilarity;
  protected final float scale_factor;
  
  protected final int termLength;
  
  protected int maxEdits;
  protected final boolean raw;

  protected final Terms terms;
  private final Term term;
  protected final int termText[];
  protected final int realPrefixLength;
  
  private final boolean transpositions;
  
  /**
   * Constructor for enumeration of all terms from specified <code>reader</code> which share a prefix of
   * length <code>prefixLength</code> with <code>term</code> and which have a fuzzy similarity &gt;
   * <code>minSimilarity</code>.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first 
   * valid term if such a term exists. 
   * 
   * @param terms Delivers terms.
   * @param atts {@link AttributeSource} created by the rewrite method of {@link MultiTermQuery}
   * thats contains information about competitive boosts during rewrite. It is also used
   * to cache DFAs between segment transitions.
   * @param term Pattern term.
   * @param minSimilarity Minimum required similarity for terms from the reader. Pass an integer value
   *        representing edit distance. Passing a fraction is deprecated.
   * @param prefixLength Length of required common prefix. Default value is 0.
   * @throws IOException if there is a low-level IO error
   */
  public FuzzyTermsEnum(Terms terms, AttributeSource atts, Term term, 
      final float minSimilarity, final int prefixLength, boolean transpositions) throws IOException {
    if (minSimilarity >= 1.0f && minSimilarity != (int)minSimilarity)
      throw new IllegalArgumentException("fractional edit distances are not allowed");
    if (minSimilarity < 0.0f)
      throw new IllegalArgumentException("minimumSimilarity cannot be less than 0");
    if(prefixLength < 0)
      throw new IllegalArgumentException("prefixLength cannot be less than 0");
    this.terms = terms;
    this.term = term;

    // convert the string into a utf32 int[] representation for fast comparisons
    final String utf16 = term.text();
    this.termText = new int[utf16.codePointCount(0, utf16.length())];
    for (int cp, i = 0, j = 0; i < utf16.length(); i += Character.charCount(cp))
           termText[j++] = cp = utf16.codePointAt(i);
    this.termLength = termText.length;
    this.dfaAtt = atts.addAttribute(LevenshteinAutomataAttribute.class);

    //The prefix could be longer than the word.
    //It's kind of silly though.  It means we must match the entire word.
    this.realPrefixLength = prefixLength > termLength ? termLength : prefixLength;
    // if minSimilarity >= 1, we treat it as number of edits
    if (minSimilarity >= 1f) {
      this.minSimilarity = 0; // just driven by number of edits
      maxEdits = (int) minSimilarity;
      raw = true;
    } else {
      this.minSimilarity = minSimilarity;
      // calculate the maximum k edits for this similarity
      maxEdits = initialMaxDistance(this.minSimilarity, termLength);
      raw = false;
    }
    if (transpositions && maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      throw new UnsupportedOperationException("with transpositions enabled, distances > " 
        + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + " are not supported ");
    }
    this.transpositions = transpositions;
    this.scale_factor = 1.0f / (1.0f - this.minSimilarity);

    this.maxBoostAtt = atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);
    bottom = maxBoostAtt.getMaxNonCompetitiveBoost();
    bottomTerm = maxBoostAtt.getCompetitiveTerm();
    bottomChanged(null, true);
  }
  
  /**
   * return an automata-based enum for matching up to editDistance from
   * lastTerm, if possible
   */
  protected TermsEnum getAutomatonEnum(int editDistance, BytesRef lastTerm)
      throws IOException {
    final List<CompiledAutomaton> runAutomata = initAutomata(editDistance);
    if (editDistance < runAutomata.size()) {
      //System.out.println("FuzzyTE.getAEnum: ed=" + editDistance + " lastTerm=" + (lastTerm==null ? "null" : lastTerm.utf8ToString()));
      final CompiledAutomaton compiled = runAutomata.get(editDistance);
      return new AutomatonFuzzyTermsEnum(terms.intersect(compiled, lastTerm == null ? null : compiled.floor(lastTerm, new BytesRefBuilder())),
                                         runAutomata.subList(0, editDistance + 1).toArray(new CompiledAutomaton[editDistance + 1]));
    } else {
      return null;
    }
  }

  /** initialize levenshtein DFAs up to maxDistance, if possible */
  private List<CompiledAutomaton> initAutomata(int maxDistance) {
    final List<CompiledAutomaton> runAutomata = dfaAtt.automata();
    //System.out.println("cached automata size: " + runAutomata.size());
    if (runAutomata.size() <= maxDistance &&
        maxDistance <= LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      LevenshteinAutomata builder = 
        new LevenshteinAutomata(UnicodeUtil.newString(termText, realPrefixLength, termText.length - realPrefixLength), transpositions);

      String prefix = UnicodeUtil.newString(termText, 0, realPrefixLength);
      for (int i = runAutomata.size(); i <= maxDistance; i++) {
        Automaton a = builder.toAutomaton(i, prefix);
        //System.out.println("compute automaton n=" + i);
        runAutomata.add(new CompiledAutomaton(a, true, false));
      }
    }
    return runAutomata;
  }

  /** swap in a new actual enum to proxy to */
  protected void setEnum(TermsEnum actualEnum) {
    this.actualEnum = actualEnum;
    this.actualBoostAtt = actualEnum.attributes().addAttribute(BoostAttribute.class);
  }
  
  /**
   * fired when the max non-competitive boost has changed. this is the hook to
   * swap in a smarter actualEnum
   */
  private void bottomChanged(BytesRef lastTerm, boolean init)
      throws IOException {
    int oldMaxEdits = maxEdits;
    
    // true if the last term encountered is lexicographically equal or after the bottom term in the PQ
    boolean termAfter = bottomTerm == null || (lastTerm != null && termComparator.compare(lastTerm, bottomTerm) >= 0);

    // as long as the max non-competitive boost is >= the max boost
    // for some edit distance, keep dropping the max edit distance.
    while (maxEdits > 0 && (termAfter ? bottom >= calculateMaxBoost(maxEdits) : bottom > calculateMaxBoost(maxEdits)))
      maxEdits--;
    
    if (oldMaxEdits != maxEdits || init) { // the maximum n has changed
      maxEditDistanceChanged(lastTerm, maxEdits, init);
    }
  }
  
  protected void maxEditDistanceChanged(BytesRef lastTerm, int maxEdits, boolean init)
      throws IOException {
    TermsEnum newEnum = getAutomatonEnum(maxEdits, lastTerm);
    // instead of assert, we do a hard check in case someone uses our enum directly
    // assert newEnum != null;
    if (newEnum == null) {
      assert maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
      throw new IllegalArgumentException("maxEdits cannot be > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE");
    }
    setEnum(newEnum);
  }

  // for some raw min similarity and input term length, the maximum # of edits
  private int initialMaxDistance(float minimumSimilarity, int termLen) {
    return (int) ((1D-minimumSimilarity) * termLen);
  }
  
  // for some number of edits, the maximum possible scaled boost
  private float calculateMaxBoost(int nEdits) {
    final float similarity = 1.0f - ((float) nEdits / (float) (termLength));
    return (similarity - minSimilarity) * scale_factor;
  }

  private BytesRef queuedBottom = null;
  
  @Override
  public BytesRef next() throws IOException {
    if (queuedBottom != null) {
      bottomChanged(queuedBottom, false);
      queuedBottom = null;
    }
    
    BytesRef term = actualEnum.next();
    boostAtt.setBoost(actualBoostAtt.getBoost());
    
    final float bottom = maxBoostAtt.getMaxNonCompetitiveBoost();
    final BytesRef bottomTerm = maxBoostAtt.getCompetitiveTerm();
    if (term != null && (bottom != this.bottom || bottomTerm != this.bottomTerm)) {
      this.bottom = bottom;
      this.bottomTerm = bottomTerm;
      // clone the term before potentially doing something with it
      // this is a rare but wonderful occurrence anyway
      queuedBottom = BytesRef.deepCopyOf(term);
    }
    
    return term;
  }
  
  // proxy all other enum calls to the actual enum
  @Override
  public int docFreq() throws IOException {
    return actualEnum.docFreq();
  }

  @Override
  public long totalTermFreq() throws IOException {
    return actualEnum.totalTermFreq();
  }
  
  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    return actualEnum.postings(reuse, flags);
  }
  
  @Override
  public void seekExact(BytesRef term, TermState state) throws IOException {
    actualEnum.seekExact(term, state);
  }
  
  @Override
  public TermState termState() throws IOException {
    return actualEnum.termState();
  }
  
  @Override
  public long ord() throws IOException {
    return actualEnum.ord();
  }
  
  @Override
  public boolean seekExact(BytesRef text) throws IOException {
    return actualEnum.seekExact(text);
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) throws IOException {
    return actualEnum.seekCeil(text);
  }
  
  @Override
  public void seekExact(long ord) throws IOException {
    actualEnum.seekExact(ord);
  }
  
  @Override
  public BytesRef term() throws IOException {
    return actualEnum.term();
  }

  /**
   * Implement fuzzy enumeration with Terms.intersect.
   * <p>
   * This is the fastest method as opposed to LinearFuzzyTermsEnum:
   * as enumeration is logarithmic to the number of terms (instead of linear)
   * and comparison is linear to length of the term (rather than quadratic)
   */
  private class AutomatonFuzzyTermsEnum extends FilteredTermsEnum {
    private final ByteRunAutomaton matchers[];
    
    private final BytesRef termRef;
    
    private final BoostAttribute boostAtt =
      attributes().addAttribute(BoostAttribute.class);
    
    public AutomatonFuzzyTermsEnum(TermsEnum tenum, CompiledAutomaton compiled[]) {
      super(tenum, false);
      this.matchers = new ByteRunAutomaton[compiled.length];
      for (int i = 0; i < compiled.length; i++)
        this.matchers[i] = compiled[i].runAutomaton;
      termRef = new BytesRef(term.text());
    }

    /** finds the smallest Lev(n) DFA that accepts the term. */
    @Override
    protected AcceptStatus accept(BytesRef term) {    
      //System.out.println("AFTE.accept term=" + term);
      int ed = matchers.length - 1;
      
      // we are wrapping either an intersect() TermsEnum or an AutomatonTermsENum,
      // so we know the outer DFA always matches.
      // now compute exact edit distance
      while (ed > 0) {
        if (matches(term, ed - 1)) {
          ed--;
        } else {
          break;
        }
      }
      //System.out.println("CHECK term=" + term.utf8ToString() + " ed=" + ed);
      
      // scale to a boost and return (if similarity > minSimilarity)
      if (ed == 0) { // exact match
        boostAtt.setBoost(1.0F);
        //System.out.println("  yes");
        return AcceptStatus.YES;
      } else {
        final int codePointCount = UnicodeUtil.codePointCount(term);
        final float similarity = 1.0f - ((float) ed / (float) 
            (Math.min(codePointCount, termLength)));
        if (similarity > minSimilarity) {
          boostAtt.setBoost((similarity - minSimilarity) * scale_factor);
          //System.out.println("  yes");
          return AcceptStatus.YES;
        } else {
          return AcceptStatus.NO;
        }
      }
    }
    
    /** returns true if term is within k edits of the query term */
    final boolean matches(BytesRef term, int k) {
      return k == 0 ? term.equals(termRef) : matchers[k].run(term.bytes, term.offset, term.length);
    }
  }

  /** @lucene.internal */
  public float getMinSimilarity() {
    return minSimilarity;
  }
  
  /** @lucene.internal */
  public float getScaleFactor() {
    return scale_factor;
  }
  
  /**
   * reuses compiled automata across different segments,
   * because they are independent of the index
   * @lucene.internal */
  public static interface LevenshteinAutomataAttribute extends Attribute {
    public List<CompiledAutomaton> automata();
  }
    
  /** 
   * Stores compiled automata as a list (indexed by edit distance)
   * @lucene.internal */
  public static final class LevenshteinAutomataAttributeImpl extends AttributeImpl implements LevenshteinAutomataAttribute {
    private final List<CompiledAutomaton> automata = new ArrayList<>();
      
    @Override
    public List<CompiledAutomaton> automata() {
      return automata;
    }

    @Override
    public void clear() {
      automata.clear();
    }

    @Override
    public int hashCode() {
      return automata.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (!(other instanceof LevenshteinAutomataAttributeImpl))
        return false;
      return automata.equals(((LevenshteinAutomataAttributeImpl) other).automata);
    }

    @Override
    public void copyTo(AttributeImpl target) {
      final List<CompiledAutomaton> targetAutomata =
        ((LevenshteinAutomataAttribute) target).automata();
      targetAutomata.clear();
      targetAutomata.addAll(automata);
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(LevenshteinAutomataAttribute.class, "automata", automata);
    }
  }
}
