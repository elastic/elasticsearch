package org.apache.lucene.util.packed;

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

import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts.Mutable;

/**
 * A {@link XPagedGrowableWriter}. This class slices data into fixed-size blocks
 * which have independent numbers of bits per value and grow on-demand.
 * <p>You should use this class instead of {@link AppendingLongBuffer} only when
 * you need random write-access. Otherwise this class will likely be slower and
 * less memory-efficient.
 * @lucene.internal
 */
public final class XPagedGrowableWriter extends XAbstractPagedMutable<XPagedGrowableWriter> {

    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1492640.
        assert Lucene.VERSION == Version.LUCENE_43 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }

  final float acceptableOverheadRatio;

  /**
   * Create a new {@link XPagedGrowableWriter} instance.
   *
   * @param size the number of values to store.
   * @param pageSize the number of values per page
   * @param startBitsPerValue the initial number of bits per value
   * @param acceptableOverheadRatio an acceptable overhead ratio
   */
  public XPagedGrowableWriter(long size, int pageSize,
      int startBitsPerValue, float acceptableOverheadRatio) {
    this(size, pageSize, startBitsPerValue, acceptableOverheadRatio, true);
  }

  XPagedGrowableWriter(long size, int pageSize,int startBitsPerValue, float acceptableOverheadRatio, boolean fillPages) {
    super(startBitsPerValue, size, pageSize);
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    if (fillPages) {
      fillPages();
    }
  }

  @Override
  protected Mutable newMutable(int valueCount, int bitsPerValue) {
    return new XGrowableWriter(bitsPerValue, valueCount, acceptableOverheadRatio);
  }

  @Override
  protected XPagedGrowableWriter newUnfilledCopy(long newSize) {
    return new XPagedGrowableWriter(newSize, pageSize(), bitsPerValue, acceptableOverheadRatio, false);
  }

  @Override
  protected long baseRamBytesUsed() {
    return super.baseRamBytesUsed() + RamUsageEstimator.NUM_BYTES_FLOAT;
  }

}
