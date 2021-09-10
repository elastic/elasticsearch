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
package org.apache.lucene5_shaded.document;


import org.apache.lucene5_shaded.analysis.NumericTokenStream; // javadocs
import org.apache.lucene5_shaded.index.IndexOptions;
import org.apache.lucene5_shaded.search.NumericRangeQuery; // javadocs
import org.apache.lucene5_shaded.util.NumericUtils;

/**
 * <p>
 * Field that indexes <code>int</code> values
 * for efficient range filtering and sorting. Here's an example usage:
 * 
 * <pre class="prettyprint">
 * document.add(new IntField(name, 6, Field.Store.NO));
 * </pre>
 * 
 * For optimal performance, re-use the <code>IntField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre class="prettyprint">
 *  IntField field = new IntField(name, 6, Field.Store.NO);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setIntValue(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 *
 * See also {@link LongField}, {@link FloatField}, {@link
 * DoubleField}.
 *
 * <p>To perform range querying or filtering against a
 * <code>IntField</code>, use {@link NumericRangeQuery}.
 * To sort according to a
 * <code>IntField</code>, use the normal numeric sort types, eg
 * {@link org.apache.lucene5_shaded.search.SortField.Type#INT}. <code>IntField</code>
 * values can also be loaded directly from {@link org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues}.</p>
 *
 * <p>You may add the same field name as an <code>IntField</code> to
 * the same document more than once.  Range querying and
 * filtering will be the logical OR of all values; so a range query
 * will hit all documents that have at least one value in
 * the range. However sort behavior is not defined.  If you need to sort,
 * you should separately index a single-valued <code>IntField</code>.</p>
 *
 * <p>An <code>IntField</code> will consume somewhat more disk space
 * in the index than an ordinary single-valued field.
 * However, for a typical index that includes substantial
 * textual content per document, this increase will likely
 * be in the noise. </p>
 *
 * <p>Within Lucene, each numeric value is indexed as a
 * <em>trie</em> structure, where each term is logically
 * assigned to larger and larger pre-defined brackets (which
 * are simply lower-precision representations of the value).
 * The step size between each successive bracket is called the
 * <code>precisionStep</code>, measured in bits.  Smaller
 * <code>precisionStep</code> values result in larger number
 * of brackets, which consumes more disk space in the index
 * but may result in faster range search performance.  The
 * default value, 8, was selected for a reasonable tradeoff
 * of disk space consumption versus performance.  You can
 * create a custom {@link FieldType} and invoke the {@link
 * FieldType#setNumericPrecisionStep} method if you'd
 * like to change the value.  Note that you must also
 * specify a congruent value when creating {@link
 * NumericRangeQuery}.
 * For low cardinality fields larger precision steps are good.
 * If the cardinality is &lt; 100, it is fair
 * to use {@link Integer#MAX_VALUE}, which produces one
 * term per value.
 *
 * <p>For more information on the internals of numeric trie
 * indexing, including the <a
 * href="../search/NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>
 * configuration, see {@link NumericRangeQuery}. The format of
 * indexed values is described in {@link NumericUtils}.
 *
 * <p>If you only need to sort by numeric value, and never
 * run range querying/filtering, you can index using a
 * <code>precisionStep</code> of {@link Integer#MAX_VALUE}.
 * This will minimize disk space consumed. </p>
 *
 * <p>More advanced users can instead use {@link
 * NumericTokenStream} directly, when indexing numbers. This
 * class is a wrapper around this token stream type for
 * easier, more intuitive usage.</p>
 *
 * @since 2.9
 */

public final class IntField extends Field {
  
  /** 
   * Type for an IntField that is not stored:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_NOT_STORED = new FieldType();
  static {
    TYPE_NOT_STORED.setTokenized(true);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_NOT_STORED.setNumericType(FieldType.NumericType.INT);
    TYPE_NOT_STORED.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
    TYPE_NOT_STORED.freeze();
  }

  /** 
   * Type for a stored IntField:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_STORED = new FieldType();
  static {
    TYPE_STORED.setTokenized(true);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_STORED.setNumericType(FieldType.NumericType.INT);
    TYPE_STORED.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
    TYPE_STORED.setStored(true);
    TYPE_STORED.freeze();
  }

  /** Creates a stored or un-stored IntField with the provided value
   *  and default <code>precisionStep</code> {@link
   *  NumericUtils#PRECISION_STEP_DEFAULT_32} (8). 
   *  @param name field name
   *  @param value 32-bit integer value
   *  @param stored Store.YES if the content should also be stored
   *  @throws IllegalArgumentException if the field name is null.
   */
  public IntField(String name, int value, Store stored) {
    super(name, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
    fieldsData = Integer.valueOf(value);
  }
  
  /** Expert: allows you to customize the {@link
   *  FieldType}. 
   *  @param name field name
   *  @param value 32-bit integer value
   *  @param type customized field type: must have {@link FieldType#numericType()}
   *         of {@link FieldType.NumericType#INT}.
   *  @throws IllegalArgumentException if the field name or type is null, or
   *          if the field type does not have a INT numericType()
   */
  public IntField(String name, int value, FieldType type) {
    super(name, type);
    if (type.numericType() != FieldType.NumericType.INT) {
      throw new IllegalArgumentException("type.numericType() must be INT but got " + type.numericType());
    }
    fieldsData = Integer.valueOf(value);
  }
}
