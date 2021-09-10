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


import java.io.IOException;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.NormsConsumer;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentWriteState;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.util.IOUtils;

import static org.apache.lucene5_shaded.codecs.lucene53.Lucene53NormsFormat.VERSION_CURRENT;

/**
 * Writer for {@link Lucene53NormsFormat}
 */
class Lucene53NormsConsumer extends NormsConsumer { 
  IndexOutput data, meta;
  final int maxDoc;

  Lucene53NormsConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(meta, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void addNormsField(FieldInfo field, Iterable<Number> values) throws IOException {
    meta.writeVInt(field.number);
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    int count = 0;

    for (Number nv : values) {
      if (nv == null) {
        throw new IllegalStateException("illegal norms data for field " + field.name + ", got null for value: " + count);
      }
      final long v = nv.longValue();
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
      count++;
    }

    if (count != maxDoc) {
      throw new IllegalStateException("illegal norms data for field " + field.name + ", expected count=" + maxDoc + ", got=" + count);
    }

    if (minValue == maxValue) {
      addConstant(minValue);
    } else if (minValue >= Byte.MIN_VALUE && maxValue <= Byte.MAX_VALUE) {
      addByte1(values);
    } else if (minValue >= Short.MIN_VALUE && maxValue <= Short.MAX_VALUE) {
      addByte2(values);
    } else if (minValue >= Integer.MIN_VALUE && maxValue <= Integer.MAX_VALUE) {
      addByte4(values);
    } else {
      addByte8(values);
    }
  }

  private void addConstant(long constant) throws IOException {
    meta.writeByte((byte) 0);
    meta.writeLong(constant);
  }

  private void addByte1(Iterable<Number> values) throws IOException {
    meta.writeByte((byte) 1);
    meta.writeLong(data.getFilePointer());

    for (Number value : values) {
      data.writeByte(value.byteValue());
    }
  }

  private void addByte2(Iterable<Number> values) throws IOException {
    meta.writeByte((byte) 2);
    meta.writeLong(data.getFilePointer());

    for (Number value : values) {
      data.writeShort(value.shortValue());
    }
  }

  private void addByte4(Iterable<Number> values) throws IOException {
    meta.writeByte((byte) 4);
    meta.writeLong(data.getFilePointer());

    for (Number value : values) {
      data.writeInt(value.intValue());
    }
  }

  private void addByte8(Iterable<Number> values) throws IOException {
    meta.writeByte((byte) 8);
    meta.writeLong(data.getFilePointer());

    for (Number value : values) {
      data.writeLong(value.longValue());
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeVInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }
}
