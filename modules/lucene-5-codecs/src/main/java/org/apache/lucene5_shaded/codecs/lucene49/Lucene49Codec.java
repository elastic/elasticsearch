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
package org.apache.lucene5_shaded.codecs.lucene49;

import java.io.IOException;

import org.apache.lucene5_shaded.codecs.Codec;
import org.apache.lucene5_shaded.codecs.CompoundFormat;
import org.apache.lucene5_shaded.codecs.DocValuesFormat;
import org.apache.lucene5_shaded.codecs.FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.LiveDocsFormat;
import org.apache.lucene5_shaded.codecs.NormsConsumer;
import org.apache.lucene5_shaded.codecs.NormsFormat;
import org.apache.lucene5_shaded.codecs.PostingsFormat;
import org.apache.lucene5_shaded.codecs.SegmentInfoFormat;
import org.apache.lucene5_shaded.codecs.StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40CompoundFormat;
import org.apache.lucene5_shaded.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene5_shaded.codecs.lucene41.Lucene41StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.lucene42.Lucene42TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.lucene46.Lucene46FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.lucene46.Lucene46SegmentInfoFormat;
import org.apache.lucene5_shaded.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene5_shaded.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene5_shaded.index.SegmentWriteState;

/**
 * Implements the Lucene 4.9 index format
 * @deprecated only for old 4.x segments
 */
@Deprecated
public class Lucene49Codec extends Codec {
  private final StoredFieldsFormat fieldsFormat = new Lucene41StoredFieldsFormat();
  private final TermVectorsFormat vectorsFormat = new Lucene42TermVectorsFormat();
  private final FieldInfosFormat fieldInfosFormat = new Lucene46FieldInfosFormat();
  private final SegmentInfoFormat segmentInfosFormat = new Lucene46SegmentInfoFormat();
  private final LiveDocsFormat liveDocsFormat = new Lucene40LiveDocsFormat();
  private final CompoundFormat compoundFormat = new Lucene40CompoundFormat();
  
  private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return Lucene49Codec.this.getPostingsFormatForField(field);
    }
  };
  
  private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
      return Lucene49Codec.this.getDocValuesFormatForField(field);
    }
  };

  /** Sole constructor. */
  public Lucene49Codec() {
    super("Lucene49");
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
  public final FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }
  
  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return segmentInfosFormat;
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
  
  /** Returns the docvalues format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Lucene49"
   */
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return defaultDVFormat;
  }
  
  @Override
  public final DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  private final PostingsFormat defaultFormat = PostingsFormat.forName("Lucene41");
  private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Lucene49");

  private final NormsFormat normsFormat = new Lucene49NormsFormat() {
    @Override
    public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
      throw new UnsupportedOperationException("this codec can only be used for reading");
    }
  };

  @Override
  public NormsFormat normsFormat() {
   return normsFormat;
  }
}
