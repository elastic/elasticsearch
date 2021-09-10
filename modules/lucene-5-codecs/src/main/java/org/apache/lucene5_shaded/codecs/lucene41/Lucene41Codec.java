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
package org.apache.lucene5_shaded.codecs.lucene41;

import org.apache.lucene5_shaded.codecs.Codec;
import org.apache.lucene5_shaded.codecs.CompoundFormat;
import org.apache.lucene5_shaded.codecs.FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.LiveDocsFormat;
import org.apache.lucene5_shaded.codecs.PostingsFormat;
import org.apache.lucene5_shaded.codecs.SegmentInfoFormat;
import org.apache.lucene5_shaded.codecs.DocValuesFormat;
import org.apache.lucene5_shaded.codecs.NormsFormat;
import org.apache.lucene5_shaded.codecs.StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40CompoundFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40DocValuesFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40NormsFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40SegmentInfoFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.perfield.PerFieldPostingsFormat;

/**
 * Implements the Lucene 4.1 index format
 * @deprecated Only for reading old 4.1 segments
 */
@Deprecated
public class Lucene41Codec extends Codec {
  private final StoredFieldsFormat fieldsFormat = new Lucene41StoredFieldsFormat();
  private final TermVectorsFormat vectorsFormat = new Lucene40TermVectorsFormat();
  private final FieldInfosFormat fieldInfosFormat = new Lucene40FieldInfosFormat();
  private final SegmentInfoFormat infosFormat = new Lucene40SegmentInfoFormat();
  private final LiveDocsFormat liveDocsFormat = new Lucene40LiveDocsFormat();
  private final CompoundFormat compoundFormat = new Lucene40CompoundFormat();
  
  private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return Lucene41Codec.this.getPostingsFormatForField(field);
    }
  };

  /** Sole constructor. */
  public Lucene41Codec() {
    super("Lucene41");
  }
  
  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return fieldsFormat;
  }
  
  @Override
  public TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }

  @Override
  public final PostingsFormat postingsFormat() {
    return postingsFormat;
  }
  
  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }
  
  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return infosFormat;
  }
  
  @Override
  public final LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }
  
  @Override
  public CompoundFormat compoundFormat() {
    return compoundFormat;
  }

  /** Returns the postings format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Lucene41"
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }
  
  @Override
  public DocValuesFormat docValuesFormat() {
    return dvFormat;
  }

  private final PostingsFormat defaultFormat = PostingsFormat.forName("Lucene41");
  private final DocValuesFormat dvFormat = new Lucene40DocValuesFormat();
  private final NormsFormat normsFormat = new Lucene40NormsFormat();

  @Override
  public NormsFormat normsFormat() {
    return normsFormat;
  }
}
