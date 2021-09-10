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
package org.apache.lucene5_shaded.codecs.lucene50;


import java.io.IOException;
import java.util.Collection;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.LiveDocsFormat;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentCommitInfo;
import org.apache.lucene5_shaded.store.ChecksumIndexInput;
import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.FixedBitSet;
import org.apache.lucene5_shaded.util.MutableBits;

/** 
 * Lucene 5.0 live docs format 
 * <p>The .liv file is optional, and only exists when a segment contains
 * deletions.
 * <p>Although per-segment, this file is maintained exterior to compound segment
 * files.
 * <p>Deletions (.liv) --&gt; IndexHeader,Generation,Bits
 * <ul>
 *   <li>SegmentHeader --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>Bits --&gt; &lt;{@link DataOutput#writeLong Int64}&gt; <sup>LongCount</sup></li>
 * </ul>
 */
public final class Lucene50LiveDocsFormat extends LiveDocsFormat {
  
  /** Sole constructor. */
  public Lucene50LiveDocsFormat() {
  }
  
  /** extension of live docs */
  private static final String EXTENSION = "liv";
  
  /** codec of live docs */
  private static final String CODEC_NAME = "Lucene50LiveDocs";
  
  /** supported version range */
  private static final int VERSION_START = 0;
  private static final int VERSION_CURRENT = VERSION_START;

  @Override
  public MutableBits newLiveDocs(int size) throws IOException {
    FixedBitSet bits = new FixedBitSet(size);
    bits.set(0, size);
    return bits;
  }

  @Override
  public MutableBits newLiveDocs(Bits existing) throws IOException {
    FixedBitSet fbs = (FixedBitSet) existing;
    return fbs.clone();
  }

  @Override
  public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException {
    long gen = info.getDelGen();
    String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);
    final int length = info.info.maxDoc();
    try (ChecksumIndexInput input = dir.openChecksumInput(name, context)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(input, CODEC_NAME, VERSION_START, VERSION_CURRENT, 
                                     info.info.getId(), Long.toString(gen, Character.MAX_RADIX));
        long data[] = new long[FixedBitSet.bits2words(length)];
        for (int i = 0; i < data.length; i++) {
          data[i] = input.readLong();
        }
        FixedBitSet fbs = new FixedBitSet(data, length);
        if (fbs.length() - fbs.cardinality() != info.getDelCount()) {
          throw new CorruptIndexException("bits.deleted=" + (fbs.length() - fbs.cardinality()) + 
                                          " info.delcount=" + info.getDelCount(), input);
        }
        return fbs;
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
    }
    throw new AssertionError();
  }

  @Override
  public void writeLiveDocs(MutableBits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) throws IOException {
    long gen = info.getNextDelGen();
    String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);
    FixedBitSet fbs = (FixedBitSet) bits;
    if (fbs.length() - fbs.cardinality() != info.getDelCount() + newDelCount) {
      throw new CorruptIndexException("bits.deleted=" + (fbs.length() - fbs.cardinality()) + 
                                      " info.delcount=" + info.getDelCount() + " newdelcount=" + newDelCount, name);
    }
    long data[] = fbs.getBits();
    try (IndexOutput output = dir.createOutput(name, context)) {
      CodecUtil.writeIndexHeader(output, CODEC_NAME, VERSION_CURRENT, info.info.getId(), Long.toString(gen, Character.MAX_RADIX));
      for (int i = 0; i < data.length; i++) {
        output.writeLong(data[i]);
      }
      CodecUtil.writeFooter(output);
    }
  }

  @Override
  public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
    if (info.hasDeletions()) {
      files.add(IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, info.getDelGen()));
    }
  }
}
