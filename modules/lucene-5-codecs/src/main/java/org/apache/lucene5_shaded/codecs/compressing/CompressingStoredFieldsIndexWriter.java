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
package org.apache.lucene5_shaded.codecs.compressing;


import static org.apache.lucene5_shaded.util.BitUtil.zigZagEncode;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene5_shaded.codecs.Codec;
import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/**
 * Efficient index format for block-based {@link Codec}s.
 * <p> This writer generates a file which can be loaded into memory using
 * memory-efficient data structures to quickly locate the block that contains
 * any document.
 * <p>In order to have a compact in-memory representation, for every block of
 * 1024 chunks, this index computes the average number of bytes per
 * chunk and for every chunk, only stores the difference between<ul>
 * <li>${chunk number} * ${average length of a chunk}</li>
 * <li>and the actual start offset of the chunk</li></ul>
 * <p>Data is written as follows:
 * <ul>
 * <li>PackedIntsVersion, &lt;Block&gt;<sup>BlockCount</sup>, BlocksEndMarker</li>
 * <li>PackedIntsVersion --&gt; {@link PackedInts#VERSION_CURRENT} as a {@link DataOutput#writeVInt VInt}</li>
 * <li>BlocksEndMarker --&gt; <tt>0</tt> as a {@link DataOutput#writeVInt VInt}, this marks the end of blocks since blocks are not allowed to start with <tt>0</tt></li>
 * <li>Block --&gt; BlockChunks, &lt;DocBases&gt;, &lt;StartPointers&gt;</li>
 * <li>BlockChunks --&gt; a {@link DataOutput#writeVInt VInt} which is the number of chunks encoded in the block</li>
 * <li>DocBases --&gt; DocBase, AvgChunkDocs, BitsPerDocBaseDelta, DocBaseDeltas</li>
 * <li>DocBase --&gt; first document ID of the block of chunks, as a {@link DataOutput#writeVInt VInt}</li>
 * <li>AvgChunkDocs --&gt; average number of documents in a single chunk, as a {@link DataOutput#writeVInt VInt}</li>
 * <li>BitsPerDocBaseDelta --&gt; number of bits required to represent a delta from the average using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a></li>
 * <li>DocBaseDeltas --&gt; {@link PackedInts packed} array of BlockChunks elements of BitsPerDocBaseDelta bits each, representing the deltas from the average doc base using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a>.</li>
 * <li>StartPointers --&gt; StartPointerBase, AvgChunkSize, BitsPerStartPointerDelta, StartPointerDeltas</li>
 * <li>StartPointerBase --&gt; the first start pointer of the block, as a {@link DataOutput#writeVLong VLong}</li>
 * <li>AvgChunkSize --&gt; the average size of a chunk of compressed documents, as a {@link DataOutput#writeVLong VLong}</li>
 * <li>BitsPerStartPointerDelta --&gt; number of bits required to represent a delta from the average using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a></li>
 * <li>StartPointerDeltas --&gt; {@link PackedInts packed} array of BlockChunks elements of BitsPerStartPointerDelta bits each, representing the deltas from the average start pointer using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a></li>
 * <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes
 * <ul>
 * <li>For any block, the doc base of the n-th chunk can be restored with
 * <code>DocBase + AvgChunkDocs * n + DocBaseDeltas[n]</code>.</li>
 * <li>For any block, the start pointer of the n-th chunk can be restored with
 * <code>StartPointerBase + AvgChunkSize * n + StartPointerDeltas[n]</code>.</li>
 * <li>Once data is loaded into memory, you can lookup the start pointer of any
 * document by performing two binary searches: a first one based on the values
 * of DocBase in order to find the right block, and then inside the block based
 * on DocBaseDeltas (by reconstructing the doc bases for every chunk).</li>
 * </ul>
 * @lucene.internal
 */
public final class CompressingStoredFieldsIndexWriter implements Closeable {
  
  final IndexOutput fieldsIndexOut;
  final int blockSize;
  int totalDocs;
  int blockDocs;
  int blockChunks;
  long firstStartPointer;
  long maxStartPointer;
  final int[] docBaseDeltas;
  final long[] startPointerDeltas;

  CompressingStoredFieldsIndexWriter(IndexOutput indexOutput, int blockSize) throws IOException {
    if (blockSize <= 0) {
      throw new IllegalArgumentException("blockSize must be positive");
    }
    this.blockSize = blockSize;
    this.fieldsIndexOut = indexOutput;
    reset();
    totalDocs = 0;
    docBaseDeltas = new int[blockSize];
    startPointerDeltas = new long[blockSize];
    fieldsIndexOut.writeVInt(PackedInts.VERSION_CURRENT);
  }

  private void reset() {
    blockChunks = 0;
    blockDocs = 0;
    firstStartPointer = -1; // means unset
  }

  private void writeBlock() throws IOException {
    assert blockChunks > 0;
    fieldsIndexOut.writeVInt(blockChunks);

    // The trick here is that we only store the difference from the average start
    // pointer or doc base, this helps save bits per value.
    // And in order to prevent a few chunks that would be far from the average to
    // raise the number of bits per value for all of them, we only encode blocks
    // of 1024 chunks at once
    // See LUCENE-4512

    // doc bases
    final int avgChunkDocs;
    if (blockChunks == 1) {
      avgChunkDocs = 0;
    } else {
      avgChunkDocs = Math.round((float) (blockDocs - docBaseDeltas[blockChunks - 1]) / (blockChunks - 1));
    }
    fieldsIndexOut.writeVInt(totalDocs - blockDocs); // docBase
    fieldsIndexOut.writeVInt(avgChunkDocs);
    int docBase = 0;
    long maxDelta = 0;
    for (int i = 0; i < blockChunks; ++i) {
      final int delta = docBase - avgChunkDocs * i;
      maxDelta |= zigZagEncode(delta);
      docBase += docBaseDeltas[i];
    }

    final int bitsPerDocBase = PackedInts.bitsRequired(maxDelta);
    fieldsIndexOut.writeVInt(bitsPerDocBase);
    PackedInts.Writer writer = PackedInts.getWriterNoHeader(fieldsIndexOut,
        PackedInts.Format.PACKED, blockChunks, bitsPerDocBase, 1);
    docBase = 0;
    for (int i = 0; i < blockChunks; ++i) {
      final long delta = docBase - avgChunkDocs * i;
      assert PackedInts.bitsRequired(zigZagEncode(delta)) <= writer.bitsPerValue();
      writer.add(zigZagEncode(delta));
      docBase += docBaseDeltas[i];
    }
    writer.finish();

    // start pointers
    fieldsIndexOut.writeVLong(firstStartPointer);
    final long avgChunkSize;
    if (blockChunks == 1) {
      avgChunkSize = 0;
    } else {
      avgChunkSize = (maxStartPointer - firstStartPointer) / (blockChunks - 1);
    }
    fieldsIndexOut.writeVLong(avgChunkSize);
    long startPointer = 0;
    maxDelta = 0;
    for (int i = 0; i < blockChunks; ++i) {
      startPointer += startPointerDeltas[i];
      final long delta = startPointer - avgChunkSize * i;
      maxDelta |= zigZagEncode(delta);
    }

    final int bitsPerStartPointer = PackedInts.bitsRequired(maxDelta);
    fieldsIndexOut.writeVInt(bitsPerStartPointer);
    writer = PackedInts.getWriterNoHeader(fieldsIndexOut, PackedInts.Format.PACKED,
        blockChunks, bitsPerStartPointer, 1);
    startPointer = 0;
    for (int i = 0; i < blockChunks; ++i) {
      startPointer += startPointerDeltas[i];
      final long delta = startPointer - avgChunkSize * i;
      assert PackedInts.bitsRequired(zigZagEncode(delta)) <= writer.bitsPerValue();
      writer.add(zigZagEncode(delta));
    }
    writer.finish();
  }

  void writeIndex(int numDocs, long startPointer) throws IOException {
    if (blockChunks == blockSize) {
      writeBlock();
      reset();
    }

    if (firstStartPointer == -1) {
      firstStartPointer = maxStartPointer = startPointer;
    }
    assert firstStartPointer > 0 && startPointer >= firstStartPointer;

    docBaseDeltas[blockChunks] = numDocs;
    startPointerDeltas[blockChunks] = startPointer - maxStartPointer;

    ++blockChunks;
    blockDocs += numDocs;
    totalDocs += numDocs;
    maxStartPointer = startPointer;
  }

  void finish(int numDocs, long maxPointer) throws IOException {
    if (numDocs != totalDocs) {
      throw new IllegalStateException("Expected " + numDocs + " docs, but got " + totalDocs);
    }
    if (blockChunks > 0) {
      writeBlock();
    }
    fieldsIndexOut.writeVInt(0); // end marker
    fieldsIndexOut.writeVLong(maxPointer);
    CodecUtil.writeFooter(fieldsIndexOut);
  }

  @Override
  public void close() throws IOException {
    fieldsIndexOut.close();
  }

}
