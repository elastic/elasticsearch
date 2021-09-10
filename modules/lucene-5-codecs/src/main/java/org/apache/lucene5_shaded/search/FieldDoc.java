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


import java.util.Arrays;

/**
 * Expert: A ScoreDoc which also contains information about
 * how to sort the referenced document.  In addition to the
 * document number and score, this object contains an array
 * of values for the document from the field(s) used to sort.
 * For example, if the sort criteria was to sort by fields
 * "a", "b" then "c", the <code>fields</code> object array
 * will have three elements, corresponding respectively to
 * the term values for the document in fields "a", "b" and "c".
 * The class of each element in the array will be either
 * Integer, Float or String depending on the type of values
 * in the terms of each field.
 *
 * <p>Created: Feb 11, 2004 1:23:38 PM
 *
 * @since   lucene5_shaded 1.4
 * @see ScoreDoc
 * @see TopFieldDocs
 */
public class FieldDoc extends ScoreDoc {

  /** Expert: The values which are used to sort the referenced document.
   * The order of these will match the original sort criteria given by a
   * Sort object.  Each Object will have been returned from
   * the <code>value</code> method corresponding
   * FieldComparator used to sort this field.
   * @see Sort
   * @see IndexSearcher#search(Query,int,Sort)
   */
  public Object[] fields;

  /** Expert: Creates one of these objects with empty sort information. */
  public FieldDoc(int doc, float score) {
    super (doc, score);
  }

  /** Expert: Creates one of these objects with the given sort information. */
  public FieldDoc(int doc, float score, Object[] fields) {
    super (doc, score);
    this.fields = fields;
  }
  
  /** Expert: Creates one of these objects with the given sort information. */
  public FieldDoc(int doc, float score, Object[] fields, int shardIndex) {
    super (doc, score, shardIndex);
    this.fields = fields;
  }
  
  // A convenience method for debugging.
  @Override
  public String toString() {
    // super.toString returns the doc and score information, so just add the
    // fields information
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(" fields=");
    sb.append(Arrays.toString(fields));
    return sb.toString();
  }
}
