/*
 * Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License 
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License. 
 */

package org.apache.lucene.analysis.commongrams;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

/*
 * TODO: Consider implementing https://issues.apache.org/jira/browse/LUCENE-1688 changes to stop list and associated constructors 
 */

/**
 * Construct bigrams for frequently occurring terms while indexing. Single terms
 * are still indexed too, with bigrams overlaid. This is achieved through the
 * use of {@link PositionIncrementAttribute#setPositionIncrement(int)}. Bigrams have a type
 * of {@link #GRAM_TYPE} Example:
 * <ul>
 * <li>input:"the quick brown fox"</li>
 * <li>output:|"the","the-quick"|"brown"|"fox"|</li>
 * <li>"the-quick" has a position increment of 0 so it is in the same position
 * as "the" "the-quick" has a term.type() of "gram"</li>
 * 
 * </ul>
 */

/*
 * Constructors and makeCommonSet based on similar code in StopFilter
 */
public final class CommonGramsFilter extends TokenFilter {

  public static final String GRAM_TYPE = "gram";
  private static final char SEPARATOR = '_';

  private final Set<?> commonWords;

  private final StringBuilder buffer = new StringBuilder();
  
  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
  private final TypeAttribute typeAttribute = addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAttribute = addAttribute(PositionLengthAttribute.class);

  private int lastStartOffset;
  private boolean lastWasCommon;
  private State savedState;

  /**
   * Construct a token stream filtering the given input using a Set of common
   * words to create bigrams. Outputs both unigrams with position increment and
   * bigrams with position increment 0 type=gram where one or both of the words
   * in a potential bigram are in the set of common words .
   * 
   * @param input TokenStream input in filter chain
   * @param commonWords The set of common words.
   */
  public CommonGramsFilter(Version matchVersion, TokenStream input, Set<?> commonWords) {
    super(input);
    this.commonWords = commonWords;
  }

  /**
   * Inserts bigrams for common words into a token stream. For each input token,
   * output the token. If the token and/or the following token are in the list
   * of common words also output a bigram with position increment 0 and
   * type="gram"
   *
   * TODO:Consider adding an option to not emit unigram stopwords
   * as in CDL XTF BigramStopFilter, CommonGramsQueryFilter would need to be
   * changed to work with this.
   *
   * TODO: Consider optimizing for the case of three
   * commongrams i.e "man of the year" normally produces 3 bigrams: "man-of",
   * "of-the", "the-year" but with proper management of positions we could
   * eliminate the middle bigram "of-the"and save a disk seek and a whole set of
   * position lookups.
   */
  @Override
  public boolean incrementToken() throws IOException {
    // get the next piece of input
    if (savedState != null) {
      restoreState(savedState);
      savedState = null;
      saveTermBuffer();
      return true;
    } else if (!input.incrementToken()) {
        return false;
    }
    
    /* We build n-grams before and after stopwords. 
     * When valid, the buffer always contains at least the separator.
     * If its empty, there is nothing before this stopword.
     */
    if (lastWasCommon || (isCommon() && buffer.length() > 0)) {
      savedState = captureState();
      gramToken();
      return true;      
    }

    saveTermBuffer();
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    lastWasCommon = false;
    savedState = null;
    buffer.setLength(0);
  }

  // ================================================= Helper Methods ================================================

  /**
   * Determines if the current token is a common term
   *
   * @return {@code true} if the current token is a common term, {@code false} otherwise
   */
  private boolean isCommon() {
    String token = new String(termAttribute.buffer(), 0, termAttribute.length());
    return commonWords != null && commonWords.contains(token);
  }

  /**
   * Saves this information to form the left part of a gram
   */
  private void saveTermBuffer() {
    buffer.setLength(0);
    buffer.append(termAttribute.buffer(), 0, termAttribute.length());
    buffer.append(SEPARATOR);
    lastStartOffset = offsetAttribute.startOffset();
    lastWasCommon = isCommon();
  }

  /**
   * Constructs a compound token.
   */
  private void gramToken() {
    buffer.append(termAttribute.buffer(), 0, termAttribute.length());
    int endOffset = offsetAttribute.endOffset();

    clearAttributes();

    int length = buffer.length();
    char termText[] = termAttribute.buffer();
    if (length > termText.length) {
      termText = termAttribute.resizeBuffer(length);
    }
    
    buffer.getChars(0, length, termText, 0);
    termAttribute.setLength(length);
    posIncAttribute.setPositionIncrement(0);
    posLenAttribute.setPositionLength(2); // bigram
    offsetAttribute.setOffset(lastStartOffset, endOffset);
    typeAttribute.setType(GRAM_TYPE);
    buffer.setLength(0);
  }
}
