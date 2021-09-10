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
package org.apache.lucene5_shaded.codecs.lucene53;


import java.util.Objects;

import org.apache.lucene5_shaded.codecs.Codec;
import org.apache.lucene5_shaded.codecs.CompoundFormat;
import org.apache.lucene5_shaded.codecs.DocValuesFormat;
import org.apache.lucene5_shaded.codecs.FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.FilterCodec;
import org.apache.lucene5_shaded.codecs.LiveDocsFormat;
import org.apache.lucene5_shaded.codecs.NormsFormat;
import org.apache.lucene5_shaded.codecs.PostingsFormat;
import org.apache.lucene5_shaded.codecs.SegmentInfoFormat;
import org.apache.lucene5_shaded.codecs.StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50SegmentInfoFormat;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene5_shaded.codecs.perfield.PerFieldPostingsFormat;

/**
 * Implements the Lucene 5.3 index format, with configurable per-field postings
 * and docvalues formats.
 * <p>
 * If you want to reuse functionality of this codec in another codec, extend
 * {@link FilterCodec}.
 *
 * @see org.apache.lucene5_shaded.codecs.lucene53 package documentation for file format details.
 * @deprecated Only for reading old 5.3 segments
 */
@Deprecated
public class Lucene53Codec extends Codec {
  private final TermVectorsFormat vectorsFormat = new Lucene50TermVectorsFormat();
  private final FieldInfosFormat fieldInfosFormat = new Lucene50FieldInfosFormat();
  private final SegmentInfoFormat segmentInfosFormat = new Lucene50SegmentInfoFormat();
  private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
  private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
  
  private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return Lucene53Codec.this.getPostingsFormatForField(field);
    }
  };
  
  private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
      return Lucene53Codec.this.getDocValuesFormatForField(field);
    }
  };
  
  private final StoredFieldsFormat storedFieldsFormat;

  /** 
   * Instantiates a new codec.
   */
  public Lucene53Codec() {
    this(Mode.BEST_SPEED);
  }
  
  /** 
   * Instantiates a new codec, specifying the stored fields compression
   * mode to use.
   * @param mode stored fields compression mode to use for newly 
   *             flushed/merged segments.
   */
  public Lucene53Codec(Mode mode) {
    super("Lucene53");
    this.storedFieldsFormat = new Lucene50StoredFieldsFormat(Objects.requireNonNull(mode));
  }
  
  @Override
  public final StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }
  
  @Override
  public final TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }

  @Override
  public final PostingsFormat postingsFormat() {
    return postingsFormat;
  }
  
  @Override
  public final FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }
  
  @Override
  public final SegmentInfoFormat segmentInfoFormat() {
    return segmentInfosFormat;
  }
  
  @Override
  public final LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }

  @Override
  public final CompoundFormat compoundFormat() {
    return compoundFormat;
  }

  /** Returns the postings format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Lucene50".
   *  <p>
   *  <b>WARNING:</b> if you subclass, you are responsible for index 
   *  backwards compatibility: future version of Lucene are only 
   *  guaranteed to be able to read the default implementation. 
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }
  
  /** Returns the docvalues format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Lucene50".
   *  <p>
   *  <b>WARNING:</b> if you subclass, you are responsible for index 
   *  backwards compatibility: future version of Lucene are only 
   *  guaranteed to be able to read the default implementation. 
   */
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return defaultDVFormat;
  }
  
  @Override
  public final DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  private final PostingsFormat defaultFormat = PostingsFormat.forName("Lucene50");
  private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Lucene50");

  private final NormsFormat normsFormat = new Lucene53NormsFormat();

  @Override
  public final NormsFormat normsFormat() {
    return normsFormat;
  }
}
