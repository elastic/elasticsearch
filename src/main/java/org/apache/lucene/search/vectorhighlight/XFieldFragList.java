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
import org.apache.lucene.search.vectorhighlight.XFieldPhraseList.WeightedPhraseInfo.Toffs;

import java.util.ArrayList;
import java.util.List;

/**
 * FieldFragList has a list of "frag info" that is used by FragmentsBuilder class
 * to create fragments (snippets).
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.5 IS OUT
public abstract class XFieldFragList {

  private List<WeightedFragInfo> fragInfos = new ArrayList<WeightedFragInfo>();

  /**
   * a constructor.
   * 
   * @param fragCharSize the length (number of chars) of a fragment
   */
  public XFieldFragList( int fragCharSize ){
  }

  /**
   * convert the list of WeightedPhraseInfo to WeightedFragInfo, then add it to the fragInfos
   * 
   * @param startOffset start offset of the fragment
   * @param endOffset end offset of the fragment
   * @param phraseInfoList list of WeightedPhraseInfo objects
   */
  public abstract void add( int startOffset, int endOffset, List<WeightedPhraseInfo> phraseInfoList );
  
  /**
   * return the list of WeightedFragInfos.
   * 
   * @return fragInfos.
   */ 
  public List<WeightedFragInfo> getFragInfos() {
    return fragInfos;
  }

  /**
   * List of term offsets + weight for a frag info
   */
  public static class WeightedFragInfo {

    private List<SubInfo> subInfos;
    private float totalBoost;
    private int startOffset;
    private int endOffset;

    public WeightedFragInfo( int startOffset, int endOffset, List<SubInfo> subInfos, float totalBoost ){
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.totalBoost = totalBoost;
      this.subInfos = subInfos;
    }
    
    public List<SubInfo> getSubInfos(){
      return subInfos;
    }
    
    public float getTotalBoost(){
      return totalBoost;
    }
    
    public int getStartOffset(){
      return startOffset;
    }
    
    public int getEndOffset(){
      return endOffset;
    }
    
    @Override
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append( "subInfos=(" );
      for( SubInfo si : subInfos )
        sb.append( si.toString() );
      sb.append( ")/" ).append( totalBoost ).append( '(' ).append( startOffset ).append( ',' ).append( endOffset ).append( ')' );
      return sb.toString();
    }
    
    /**
     * Represents the list of term offsets for some text
     */
    public static class SubInfo {
      private final String text;  // unnecessary member, just exists for debugging purpose
      private final List<Toffs> termsOffsets;   // usually termsOffsets.size() == 1,
                              // but if position-gap > 1 and slop > 0 then size() could be greater than 1
      private int seqnum;

      public SubInfo( String text, List<Toffs> termsOffsets, int seqnum ){
        this.text = text;
        this.termsOffsets = termsOffsets;
        this.seqnum = seqnum;
      }
      
      public List<Toffs> getTermsOffsets(){
        return termsOffsets;
      }
      
      public int getSeqnum(){
        return seqnum;
      }

      public String getText(){
        return text;
      }

      @Override
      public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append( text ).append( '(' );
        for( Toffs to : termsOffsets )
          sb.append( to.toString() );
        sb.append( ')' );
        return sb.toString();
      }
    }
  }
}
