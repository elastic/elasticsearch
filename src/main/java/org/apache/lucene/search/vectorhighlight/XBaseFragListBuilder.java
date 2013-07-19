package org.apache.lucene.search.vectorhighlight;

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

import org.apache.lucene.search.vectorhighlight.XFieldPhraseList.WeightedPhraseInfo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A abstract implementation of {@link XFragListBuilder}.
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.5 IS OUT
public abstract class XBaseFragListBuilder implements XFragListBuilder {
  
  public static final int MARGIN_DEFAULT = 6;
  public static final int MIN_FRAG_CHAR_SIZE_FACTOR = 3;

  final int margin;
  final int minFragCharSize;

  public XBaseFragListBuilder( int margin ){
    if( margin < 0 )
      throw new IllegalArgumentException( "margin(" + margin + ") is too small. It must be 0 or higher." );

    this.margin = margin;
    this.minFragCharSize = Math.max( 1, margin * MIN_FRAG_CHAR_SIZE_FACTOR );
  }

  public XBaseFragListBuilder(){
    this( MARGIN_DEFAULT );
  }
  
 protected XFieldFragList createFieldFragList( XFieldPhraseList fieldPhraseList, XFieldFragList fieldFragList, int fragCharSize ){
    if( fragCharSize < minFragCharSize )
      throw new IllegalArgumentException( "fragCharSize(" + fragCharSize + ") is too small. It must be " + minFragCharSize + " or higher." );
    
    List<WeightedPhraseInfo> wpil = new ArrayList<WeightedPhraseInfo>();
    IteratorQueue<WeightedPhraseInfo> queue = new IteratorQueue<WeightedPhraseInfo>(fieldPhraseList.getPhraseList().iterator());
    WeightedPhraseInfo phraseInfo = null;
    int startOffset = 0;
    while((phraseInfo = queue.top()) != null){
      // if the phrase violates the border of previous fragment, discard it and try next phrase
      if( phraseInfo.getStartOffset() < startOffset )  {
        queue.removeTop();
        continue;
      }
      
      wpil.clear();
      final int currentPhraseStartOffset = phraseInfo.getStartOffset();
      int currentPhraseEndOffset = phraseInfo.getEndOffset();
      int spanStart = Math.max(currentPhraseStartOffset - margin, startOffset);
      int spanEnd = Math.max(currentPhraseEndOffset, spanStart + fragCharSize);
      if (acceptPhrase(queue.removeTop(),  currentPhraseEndOffset - currentPhraseStartOffset, fragCharSize)) {
        wpil.add(phraseInfo);
      }
      while((phraseInfo = queue.top()) != null) { // pull until we crossed the current spanEnd
        if (phraseInfo.getEndOffset() <= spanEnd) {
          currentPhraseEndOffset = phraseInfo.getEndOffset();
          if (acceptPhrase(queue.removeTop(),  currentPhraseEndOffset - currentPhraseStartOffset, fragCharSize)) {
            wpil.add(phraseInfo);
          }
        } else {
          break;
        }
      }
      if (wpil.isEmpty()) {
        continue;
      }
      
      final int matchLen = currentPhraseEndOffset - currentPhraseStartOffset;
      // now recalculate the start and end position to "center" the result
      final int newMargin = Math.max(0, (fragCharSize-matchLen)/2); // matchLen can be > fragCharSize prevent IAOOB here
      spanStart = currentPhraseStartOffset - newMargin;
      if (spanStart < startOffset) {
        spanStart = startOffset;
      }
      // whatever is bigger here we grow this out
      spanEnd = spanStart + Math.max(matchLen, fragCharSize);  
      startOffset = spanEnd;
      fieldFragList.add(spanStart, spanEnd, wpil);
    }
    return fieldFragList;
  }
 
  /**
   * A predicate to decide if the given {@link WeightedPhraseInfo} should be
   * accepted as a highlighted phrase or if it should be discarded.
   * <p>
   * The default implementation discards phrases that are composed of more than one term
   * and where the matchLength exceeds the fragment character size.
   * 
   * @param info the phrase info to accept
   * @param matchLength the match length of the current phrase
   * @param fragCharSize the configured fragment character size
   * @return <code>true</code> if this phrase info should be accepted as a highligh phrase
   */
 protected boolean acceptPhrase(WeightedPhraseInfo info, int matchLength, int fragCharSize) {
   return info.getTermsOffsets().size() <= 1 ||  matchLength <= fragCharSize;
 }
 
 private static final class IteratorQueue<T> {
   private final Iterator<T> iter;
   private T top;
   
   public IteratorQueue(Iterator<T> iter) {
     this.iter = iter;
     T removeTop = removeTop();
     assert removeTop == null;
   }
   
   public T top() {
     return top;
   }
   
   public T removeTop() {
     T currentTop = top;
     if (iter.hasNext()) {
       top = iter.next();
     } else {
       top = null;
     }
     return currentTop;
   }
   
 }
 
}
