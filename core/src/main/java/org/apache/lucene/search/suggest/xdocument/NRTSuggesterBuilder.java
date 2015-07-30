package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.*;

import java.io.IOException;
import java.util.PriorityQueue;

import static org.apache.lucene.search.suggest.xdocument.NRTSuggester.encode;

/**
 * Builder for {@link NRTSuggester}
 *
 */
final class NRTSuggesterBuilder {

  /**
   * Label used to separate surface form and docID
   * in the output
   */
  public static final int PAYLOAD_SEP = '\u001F';

  /**
   * Marks end of the analyzed input and start of dedup
   * byte.
   */
  public static final int END_BYTE = 0x0;

  private final PairOutputs<Long, BytesRef> outputs;
  private final Builder<PairOutputs.Pair<Long, BytesRef>> builder;
  private final IntsRefBuilder scratchInts = new IntsRefBuilder();
  private final BytesRefBuilder analyzed = new BytesRefBuilder();
  private final PriorityQueue<Entry> entries;
  private final int payloadSep;
  private final int endByte;

  private int maxAnalyzedPathsPerOutput = 0;

  /**
   * Create a builder for {@link NRTSuggester}
   */
  public NRTSuggesterBuilder() {
    this.payloadSep = PAYLOAD_SEP;
    this.endByte = END_BYTE;
    this.outputs = new PairOutputs<>(PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton());
    this.entries = new PriorityQueue<>();
    this.builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
  }

  /**
   * Initializes an FST input term to add entries against
   */
  public void startTerm(BytesRef analyzed) {
    this.analyzed.copyBytes(analyzed);
    this.analyzed.append((byte) endByte);
  }

  /**
   * Adds an entry for the latest input term, should be called after
   * {@link #startTerm(org.apache.lucene.util.BytesRef)} on the desired input
   */
  public void addEntry(int docID, BytesRef surfaceForm, long weight) throws IOException {
    BytesRef payloadRef = NRTSuggester.PayLoadProcessor.make(surfaceForm, docID, payloadSep);
    entries.add(new Entry(payloadRef, encode(weight)));
  }

  /**
   * Writes all the entries for the FST input term
   */
  public void finishTerm() throws IOException {
    int numArcs = 0;
    int numDedupBytes = 1;
    analyzed.grow(analyzed.length() + 1);
    analyzed.setLength(analyzed.length() + 1);
    for (Entry entry : entries) {
      if (numArcs == maxNumArcsForDedupByte(numDedupBytes)) {
        analyzed.setByteAt(analyzed.length() - 1, (byte) (numArcs));
        analyzed.grow(analyzed.length() + 1);
        analyzed.setLength(analyzed.length() + 1);
        numArcs = 0;
        numDedupBytes++;
      }
      analyzed.setByteAt(analyzed.length() - 1, (byte) numArcs++);
      XUtil.toIntsRef(analyzed.get(), scratchInts);
      builder.add(scratchInts.get(), outputs.newPair(entry.weight, entry.payload));
    }
    maxAnalyzedPathsPerOutput = Math.max(maxAnalyzedPathsPerOutput, entries.size());
    entries.clear();
  }

  /**
   * Builds and stores a FST that can be loaded with
   * {@link NRTSuggester#load(org.apache.lucene.store.IndexInput)}
   */
  public boolean store(DataOutput output) throws IOException {
    final FST<PairOutputs.Pair<Long, BytesRef>> build = builder.finish();
    if (build == null) {
      return false;
    }
    build.save(output);

    /* write some more meta-info */
    assert maxAnalyzedPathsPerOutput > 0;
    output.writeVInt(maxAnalyzedPathsPerOutput);
    output.writeVInt(END_BYTE);
    output.writeVInt(PAYLOAD_SEP);
    return true;
  }

  /**
   * Num arcs for nth dedup byte:
   * if n <= 5: 1 + (2 * n)
   * else: (1 + (2 * n)) * n
   * <p>
   * TODO: is there a better way to make the fst built to be
   * more TopNSearcher friendly?
   */
  private static int maxNumArcsForDedupByte(int currentNumDedupBytes) {
    int maxArcs = 1 + (2 * currentNumDedupBytes);
    if (currentNumDedupBytes > 5) {
      maxArcs *= currentNumDedupBytes;
    }
    return Math.min(maxArcs, 255);
  }

  private final static class Entry implements Comparable<Entry> {
    final BytesRef payload;
    final long weight;

    public Entry(BytesRef payload, long weight) {
      this.payload = payload;
      this.weight = weight;
    }

    @Override
    public int compareTo(Entry o) {
      return Long.compare(weight, o.weight);
    }
  }
}
