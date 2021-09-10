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
package org.apache.lucene5_shaded.codecs.lucene42;

import java.io.IOException;

import org.apache.lucene5_shaded.codecs.NormsConsumer;
import org.apache.lucene5_shaded.codecs.NormsFormat;
import org.apache.lucene5_shaded.codecs.NormsProducer;
import org.apache.lucene5_shaded.codecs.UndeadNormsProducer;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SegmentWriteState;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/**
 * Lucene 4.2 score normalization format.
 * @deprecated only for reading old 4.x segments
 */
@Deprecated
public class Lucene42NormsFormat extends NormsFormat {
  final float acceptableOverheadRatio;

  /** 
   * Calls {@link #Lucene42NormsFormat(float) 
   * Lucene42DocValuesFormat(PackedInts.FASTEST)} 
   */
  public Lucene42NormsFormat() {
    // note: we choose FASTEST here (otherwise our norms are half as big but 15% slower than previous lucene)
    this(PackedInts.FASTEST);
  }
  
  /**
   * Creates a new Lucene42DocValuesFormat with the specified
   * <code>acceptableOverheadRatio</code> for NumericDocValues.
   * @param acceptableOverheadRatio compression parameter for numerics. 
   *        Currently this is only used when the number of unique values is small.
   *        
   * @lucene.experimental
   */
  public Lucene42NormsFormat(float acceptableOverheadRatio) {
    this.acceptableOverheadRatio = acceptableOverheadRatio;
  }
  
  @Override
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
  
  @Override
  public final NormsProducer normsProducer(SegmentReadState state) throws IOException {
    if (UndeadNormsProducer.isUndeadArmy(state.fieldInfos)) {
      return UndeadNormsProducer.INSTANCE;
    } else {
      return new Lucene42NormsProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
    }
  }
  
  static final String DATA_CODEC = "Lucene41NormsData";
  static final String DATA_EXTENSION = "nvd";
  static final String METADATA_CODEC = "Lucene41NormsMetadata";
  static final String METADATA_EXTENSION = "nvm";
}
