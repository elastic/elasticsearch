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
package org.apache.lucene.analysis.commongrams;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import static org.apache.lucene.analysis.commongrams.CommonGramsFilter.GRAM_TYPE;

/**
 * Wrap a CommonGramsFilter optimizing phrase queries by only returning single
 * words when they are not a member of a bigram.
 * 
 * Example:
 * <ul>
 * <li>query input to CommonGramsFilter: "the rain in spain falls mainly"
 * <li>output of CommomGramsFilter/input to CommonGramsQueryFilter:
 * |"the, "the-rain"|"rain" "rain-in"|"in, "in-spain"|"spain"|"falls"|"mainly"
 * <li>output of CommonGramsQueryFilter:"the-rain", "rain-in" ,"in-spain",
 * "falls", "mainly"
 * </ul>
 */

/*
 * See:http://hudson.zones.apache.org/hudson/job/Lucene-trunk/javadoc//all/org/apache/lucene/analysis/TokenStream.html and
 * http://svn.apache.org/viewvc/lucene/dev/trunk/lucene/src/java/org/apache/lucene/analysis/package.html?revision=718798
 */
public final class CommonGramsQueryFilter extends TokenFilter {

  private final TypeAttribute typeAttribute = addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);
  
  private State previous;
  private String previousType;
  private boolean exhausted;

  /**
   * Constructs a new CommonGramsQueryFilter based on the provided CommomGramsFilter 
   * 
   * @param input CommonGramsFilter the QueryFilter will use
   */
  public CommonGramsQueryFilter(CommonGramsFilter input) {
    super(input);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    previous = null;
    previousType = null;
    exhausted = false;
  }
  
  /**
   * Output bigrams whenever possible to optimize queries. Only output unigrams
   * when they are not a member of a bigram. Example:
   * <ul>
   * <li>input: "the rain in spain falls mainly"
   * <li>output:"the-rain", "rain-in" ,"in-spain", "falls", "mainly"
   * </ul>
   */
  @Override
  public boolean incrementToken() throws IOException {
    while (!exhausted && input.incrementToken()) {
      State current = captureState();

      if (previous != null && !isGramType()) {
        restoreState(previous);
        previous = current;
        previousType = typeAttribute.type();
        
        if (isGramType()) {
          posIncAttribute.setPositionIncrement(1);
        }
        return true;
      }

      previous = current;
    }

    exhausted = true;

    if (previous == null || GRAM_TYPE.equals(previousType)) {
      return false;
    }
    
    restoreState(previous);
    previous = null;
    
    if (isGramType()) {
      posIncAttribute.setPositionIncrement(1);
    }
    return true;
  }

  // ================================================= Helper Methods ================================================

  /**
   * Convenience method to check if the current type is a gram type
   * 
   * @return {@code true} if the current type is a gram type, {@code false} otherwise
   */
  public boolean isGramType() {
    return GRAM_TYPE.equals(typeAttribute.type());
  }
}
