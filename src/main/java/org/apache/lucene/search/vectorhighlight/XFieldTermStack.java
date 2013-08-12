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

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * <code>FieldTermStack</code> is a stack that keeps query terms in the specified field
 * of the document to be highlighted.
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.5 IS OUT
public class XFieldTermStack {
  
  private final String fieldName;
  LinkedList<TermInfo> termList = new LinkedList<TermInfo>();
  
  //public static void main( String[] args ) throws Exception {
  //  Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_CURRENT);
  //  QueryParser parser = new QueryParser(Version.LUCENE_CURRENT,  "f", analyzer );
  //  Query query = parser.parse( "a x:b" );
  //  FieldQuery fieldQuery = new FieldQuery( query, true, false );
    
  //  Directory dir = new RAMDirectory();
  //  IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer));
  //  Document doc = new Document();
  //  FieldType ft = new FieldType(TextField.TYPE_STORED);
  //  ft.setStoreTermVectors(true);
  //  ft.setStoreTermVectorOffsets(true);
  //  ft.setStoreTermVectorPositions(true);
  //  doc.add( new Field( "f", ft, "a a a b b c a b b c d e f" ) );
  //  doc.add( new Field( "f", ft, "b a b a f" ) );
  //  writer.addDocument( doc );
  //  writer.close();
    
  //  IndexReader reader = IndexReader.open(dir1);
  //  new FieldTermStack( reader, 0, "f", fieldQuery );
  //  reader.close();
  //}

  /**
   * a constructor.
   * 
   * @param reader IndexReader of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fieldQuery FieldQuery object
   * @throws IOException If there is a low-level I/O error
   */
  public XFieldTermStack( IndexReader reader, int docId, String fieldName, final XFieldQuery fieldQuery ) throws IOException {
    this.fieldName = fieldName;
    
    Set<String> termSet = fieldQuery.getTermSet( fieldName );
    // just return to make null snippet if un-matched fieldName specified when fieldMatch == true
    if( termSet == null ) return;

    final Fields vectors = reader.getTermVectors(docId);
    if (vectors == null) {
      // null snippet
      return;
    }

    final Terms vector = vectors.terms(fieldName);
    if (vector == null) {
      // null snippet
      return;
    }

    final CharsRef spare = new CharsRef();
    final TermsEnum termsEnum = vector.iterator(null);
    DocsAndPositionsEnum dpEnum = null;
    BytesRef text;
    
    int numDocs = reader.maxDoc();
    
    final List<TermInfo> termList = new ArrayList<TermInfo>();
    while ((text = termsEnum.next()) != null) {
      UnicodeUtil.UTF8toUTF16(text, spare);
      final String term = spare.toString();
      if (!termSet.contains(term)) {
        continue;
      }
      dpEnum = termsEnum.docsAndPositions(null, dpEnum);
      if (dpEnum == null) {
        // null snippet
        return;
      }

      dpEnum.nextDoc();
      
      // For weight look here: http://lucene.apache.org/core/3_6_0/api/core/org/apache/lucene/search/DefaultSimilarity.html
      final float weight = ( float ) ( Math.log( numDocs / ( double ) ( reader.docFreq( new Term(fieldName, text) ) + 1 ) ) + 1.0 );

      // ES EDIT: added a safety check to limit this to 512 terms everything above might be meaningless anyways
      // This limit protectes the FVH from running into StackOverflowErrors if super large TF docs are highlighted. 
      final int freq = Math.min(512, dpEnum.freq()); 
      
      
      for(int i = 0;i < freq;i++) {
        int pos = dpEnum.nextPosition();
        if (dpEnum.startOffset() < 0) {
          return; // no offsets, null snippet
        }
        termList.add( new TermInfo( term, dpEnum.startOffset(), dpEnum.endOffset(), pos, weight ) );
      }
    }
    
    // sort by position
    CollectionUtil.timSort(termList);
    this.termList.addAll(termList);
  }

  /**
   * @return field name
   */
  public String getFieldName(){
    return fieldName;
  }

  /**
   * @return the top TermInfo object of the stack
   */
  public TermInfo pop(){
    return termList.poll();
  }

  /**
   * Return the top TermInfo object of the stack without removing it.
   */
  public TermInfo peek() {
    return termList.peek();
  }

  /**
   * @param termInfo the TermInfo object to be put on the top of the stack
   */
  public void push( TermInfo termInfo ){
    termList.push( termInfo );
  }

  /**
   * to know whether the stack is empty
   * 
   * @return true if the stack is empty, false if not
   */
  public boolean isEmpty(){
    return termList == null || termList.size() == 0;
  }
  
  /**
   * Single term with its position/offsets in the document and IDF weight
   */
  public static class TermInfo implements Comparable<TermInfo>{

    private final String text;
    private final int startOffset;
    private final int endOffset;
    private final int position;    

    // IDF-weight of this term
    private final float weight;

    public TermInfo( String text, int startOffset, int endOffset, int position, float weight ){
      this.text = text;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.position = position;
      this.weight = weight;
    }
    
    public String getText(){ return text; }
    public int getStartOffset(){ return startOffset; }
    public int getEndOffset(){ return endOffset; }
    public int getPosition(){ return position; }
    public float getWeight(){ return weight; }
    
    @Override
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append( text ).append( '(' ).append(startOffset).append( ',' ).append( endOffset ).append( ',' ).append( position ).append( ')' );
      return sb.toString();
    }

    @Override
    public int compareTo( TermInfo o ){
      return ( this.position - o.position );
    }
  }
}
