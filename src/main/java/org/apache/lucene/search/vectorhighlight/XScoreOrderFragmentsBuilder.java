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

import org.apache.lucene.search.vectorhighlight.XFieldFragList.WeightedFragInfo;
import org.apache.lucene.util.CollectionUtil;

import java.util.Comparator;
import java.util.List;

/**
 * An implementation of FragmentsBuilder that outputs score-order fragments.
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.5 IS OUT
public class XScoreOrderFragmentsBuilder extends XBaseFragmentsBuilder {

  /**
   * a constructor.
   */
  public XScoreOrderFragmentsBuilder(){
    super();
  }

  /**
   * a constructor.
   * 
   * @param preTags array of pre-tags for markup terms.
   * @param postTags array of post-tags for markup terms.
   */
  public XScoreOrderFragmentsBuilder( String[] preTags, String[] postTags ){
    super( preTags, postTags );
  }

  public XScoreOrderFragmentsBuilder( BoundaryScanner bs ){
    super( bs );
  }

  public XScoreOrderFragmentsBuilder( String[] preTags, String[] postTags, BoundaryScanner bs ){
    super( preTags, postTags, bs );
  }

  /**
   * Sort by score the list of WeightedFragInfo
   */
  @Override
  public List<WeightedFragInfo> getWeightedFragInfoList( List<WeightedFragInfo> src ) {
    CollectionUtil.timSort( src, new ScoreComparator() );
    return src;
  }

  /**
   * Comparator for {@link WeightedFragInfo} by boost, breaking ties
   * by offset.
   */
  public static class ScoreComparator implements Comparator<WeightedFragInfo> {

    @Override
    public int compare( WeightedFragInfo o1, WeightedFragInfo o2 ) {
      if( o1.getTotalBoost() > o2.getTotalBoost() ) return -1;
      else if( o1.getTotalBoost() < o2.getTotalBoost() ) return 1;
      // if same score then check startOffset
      else{
        if( o1.getStartOffset() < o2.getStartOffset() ) return -1;
        else if( o1.getStartOffset() > o2.getStartOffset() ) return 1;
      }
      return 0;
    }
  }
}
