/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;

/**
 * A modification of SimpleFragListBuilder to expose the margin property. Implements FragListBuilder {@link FragListBuilder}.
 */
public class MarginFragListBuilder implements FragListBuilder {

  private static final int DEFAULT_MARGIN = 6;
  private int margin;
  private int minFragCharSize;

  public MarginFragListBuilder() {
   this(DEFAULT_MARGIN);
  }

  public MarginFragListBuilder(int startMargin) {
    margin = startMargin;
    minFragCharSize = 3*margin;
  }

  public FieldFragList createFieldFragList(FieldPhraseList fieldPhraseList, int fragCharSize) {
    if( fragCharSize < minFragCharSize )
      throw new IllegalArgumentException( "fragCharSize(" + fragCharSize + ") is too small. It must be " +
          minFragCharSize + " or higher." );

    FieldFragList ffl = new FieldFragList( fragCharSize );

    List<WeightedPhraseInfo> wpil = new ArrayList<WeightedPhraseInfo>();
    Iterator<WeightedPhraseInfo> ite = fieldPhraseList.phraseList.iterator();
    WeightedPhraseInfo phraseInfo = null;
    int startOffset = 0;
    boolean taken = false;
    while( true ){
      if( !taken ){
        if( !ite.hasNext() ) break;
        phraseInfo = ite.next();
      }
      taken = false;
      if( phraseInfo == null ) break;

      // if the phrase violates the border of previous fragment, discard it and try next phrase
      if( phraseInfo.getStartOffset() < startOffset ) continue;

      wpil.clear();
      wpil.add( phraseInfo );
      int st = phraseInfo.getStartOffset() - margin < startOffset ?
          startOffset : phraseInfo.getStartOffset() - margin;
      int en = st + fragCharSize;
      if( phraseInfo.getEndOffset() > en )
        en = phraseInfo.getEndOffset();
      startOffset = en;

      while( true ){
        if( ite.hasNext() ){
          phraseInfo = ite.next();
          taken = true;
          if( phraseInfo == null ) break;
        }
        else
          break;
        if( phraseInfo.getEndOffset() <= en )
          wpil.add( phraseInfo );
        else
          break;
      }
      ffl.add( st, en, wpil );
    }
    return ffl;
  }

}
