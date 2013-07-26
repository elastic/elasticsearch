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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Encoder;

import java.io.IOException;

/**
 * Another highlighter implementation.
 *
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.5 IS OUT
public class XFastVectorHighlighter {

  public static final boolean DEFAULT_PHRASE_HIGHLIGHT = true;
  public static final boolean DEFAULT_FIELD_MATCH = true;
  private final boolean phraseHighlight;
  private final boolean fieldMatch;
  private final XFragListBuilder fragListBuilder;
  private final XFragmentsBuilder fragmentsBuilder;
  private int phraseLimit = Integer.MAX_VALUE;

  /**
   * the default constructor.
   */
  public XFastVectorHighlighter(){
    this( DEFAULT_PHRASE_HIGHLIGHT, DEFAULT_FIELD_MATCH );
  }

  /**
   * a constructor. Using {@link XSimpleFragListBuilder} and {@link XScoreOrderFragmentsBuilder}.
   * 
   * @param phraseHighlight true or false for phrase highlighting
   * @param fieldMatch true of false for field matching
   */
  public XFastVectorHighlighter( boolean phraseHighlight, boolean fieldMatch ){
    this( phraseHighlight, fieldMatch, new XSimpleFragListBuilder(), new XScoreOrderFragmentsBuilder() );
  }

  /**
   * a constructor. A {@link XFragListBuilder} and a {@link XFragmentsBuilder} can be specified (plugins).
   * 
   * @param phraseHighlight true of false for phrase highlighting
   * @param fieldMatch true of false for field matching
   * @param fragListBuilder an instance of {@link XFragListBuilder}
   * @param fragmentsBuilder an instance of {@link XFragmentsBuilder}
   */
  public XFastVectorHighlighter( boolean phraseHighlight, boolean fieldMatch,
      XFragListBuilder fragListBuilder, XFragmentsBuilder fragmentsBuilder ){
    this.phraseHighlight = phraseHighlight;
    this.fieldMatch = fieldMatch;
    this.fragListBuilder = fragListBuilder;
    this.fragmentsBuilder = fragmentsBuilder;
  }

  /**
   * create a {@link XFieldQuery} object.
   * 
   * @param query a query
   * @return the created {@link XFieldQuery} object
   */
  public XFieldQuery getFieldQuery( Query query ) {
    // TODO: should we deprecate this? 
    // because if there is no reader, then we cannot rewrite MTQ.
    try {
      return new XFieldQuery( query, null, phraseHighlight, fieldMatch );
    } catch (IOException e) {
      // should never be thrown when reader is null
      throw new RuntimeException (e);
    }
  }
  
  /**
   * create a {@link XFieldQuery} object.
   * 
   * @param query a query
   * @return the created {@link XFieldQuery} object
   */
  public XFieldQuery getFieldQuery( Query query, IndexReader reader ) throws IOException {
    return new XFieldQuery( query, reader, phraseHighlight, fieldMatch );
  }

  /**
   * return the best fragment.
   * 
   * @param fieldQuery {@link XFieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @return the best fragment (snippet) string
   * @throws IOException If there is a low-level I/O error
   */
  public final String getBestFragment( final XFieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize ) throws IOException {
    XFieldFragList fieldFragList =
      getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragment( reader, docId, fieldName, fieldFragList );
  }

  /**
   * return the best fragments.
   * 
   * @param fieldQuery {@link XFieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @param maxNumFragments maximum number of fragments
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException If there is a low-level I/O error
   */
  public final String[] getBestFragments( final XFieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize, int maxNumFragments ) throws IOException {
    XFieldFragList fieldFragList =
      getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragments( reader, docId, fieldName, fieldFragList, maxNumFragments );
  }

  /**
   * return the best fragment.
   * 
   * @param fieldQuery {@link XFieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @param fragListBuilder {@link XFragListBuilder} object
   * @param fragmentsBuilder {@link XFragmentsBuilder} object
   * @param preTags pre-tags to be used to highlight terms
   * @param postTags post-tags to be used to highlight terms
   * @param encoder an encoder that generates encoded text
   * @return the best fragment (snippet) string
   * @throws IOException If there is a low-level I/O error
   */
  public final String getBestFragment( final XFieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize,
      XFragListBuilder fragListBuilder, XFragmentsBuilder fragmentsBuilder,
      String[] preTags, String[] postTags, Encoder encoder ) throws IOException {
    XFieldFragList fieldFragList = getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragment( reader, docId, fieldName, fieldFragList, preTags, postTags, encoder );
  }

  /**
   * return the best fragments.
   * 
   * @param fieldQuery {@link XFieldQuery} object
   * @param reader {@link IndexReader} of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fragCharSize the length (number of chars) of a fragment
   * @param maxNumFragments maximum number of fragments
   * @param fragListBuilder {@link XFragListBuilder} object
   * @param fragmentsBuilder {@link XFragmentsBuilder} object
   * @param preTags pre-tags to be used to highlight terms
   * @param postTags post-tags to be used to highlight terms
   * @param encoder an encoder that generates encoded text
   * @return created fragments or null when no fragments created.
   *         size of the array can be less than maxNumFragments
   * @throws IOException If there is a low-level I/O error
   */
  public final String[] getBestFragments( final XFieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize, int maxNumFragments,
      XFragListBuilder fragListBuilder, XFragmentsBuilder fragmentsBuilder,
      String[] preTags, String[] postTags, Encoder encoder ) throws IOException {
    XFieldFragList fieldFragList =
      getFieldFragList( fragListBuilder, fieldQuery, reader, docId, fieldName, fragCharSize );
    return fragmentsBuilder.createFragments( reader, docId, fieldName, fieldFragList, maxNumFragments,
        preTags, postTags, encoder );
  }
  
  private XFieldFragList getFieldFragList( XFragListBuilder fragListBuilder,
      final XFieldQuery fieldQuery, IndexReader reader, int docId,
      String fieldName, int fragCharSize ) throws IOException {
    XFieldTermStack fieldTermStack = new XFieldTermStack( reader, docId, fieldName, fieldQuery );
    XFieldPhraseList fieldPhraseList = new XFieldPhraseList( fieldTermStack, fieldQuery, phraseLimit );
    return fragListBuilder.createFieldFragList( fieldPhraseList, fragCharSize );
  }

  /**
   * return whether phraseHighlight or not.
   * 
   * @return whether phraseHighlight or not
   */
  public boolean isPhraseHighlight(){ return phraseHighlight; }

  /**
   * return whether fieldMatch or not.
   * 
   * @return whether fieldMatch or not
   */
  public boolean isFieldMatch(){ return fieldMatch; }
  
  /**
   * @return the maximum number of phrases to analyze when searching for the highest-scoring phrase.
   */
  public int getPhraseLimit () { return phraseLimit; }
  
  /**
   * set the maximum number of phrases to analyze when searching for the highest-scoring phrase.
   * The default is unlimited (Integer.MAX_VALUE).
   */
  public void setPhraseLimit (int phraseLimit) { this.phraseLimit = phraseLimit; }
}
