/*
 * @notice
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
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.ByteSequenceOutputs;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.FST;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.OffHeapFSTStore;

import java.io.IOException;

/**
 * BlockTree's implementation of {@link Terms}.
 *
 * This is a fork of {@link org.apache.lucene.backward_codecs.lucene40.blocktree.FieldReader} that allows to read from ancient
 * Lucene versions. The key difference is the FST loading which relies on {@link FST}.
 */
public final class FieldReader extends Terms {

    // private final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

    final long numTerms;
    final FieldInfo fieldInfo;
    final long sumTotalTermFreq;
    final long sumDocFreq;
    final int docCount;
    final long rootBlockFP;
    final BytesRef rootCode;
    final BytesRef minTerm;
    final BytesRef maxTerm;
    final Lucene40BlockTreeTermsReader parent;

    final FST<BytesRef> index;
    // private boolean DEBUG;

    FieldReader(
        Lucene40BlockTreeTermsReader parent,
        FieldInfo fieldInfo,
        long numTerms,
        BytesRef rootCode,
        long sumTotalTermFreq,
        long sumDocFreq,
        int docCount,
        long indexStartFP,
        IndexInput metaIn,
        IndexInput indexIn,
        BytesRef minTerm,
        BytesRef maxTerm
    ) throws IOException {
        assert numTerms > 0;
        this.fieldInfo = fieldInfo;
        // DEBUG = BlockTreeTermsReader.DEBUG && fieldInfo.name.equals("id");
        this.parent = parent;
        this.numTerms = numTerms;
        this.sumTotalTermFreq = sumTotalTermFreq;
        this.sumDocFreq = sumDocFreq;
        this.docCount = docCount;
        this.rootCode = rootCode;
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        // if (DEBUG) {
        // System.out.println("BTTR: seg=" + segment + " field=" + fieldInfo.name + " rootBlockCode="
        // + rootCode + " divisor=" + indexDivisor);
        // }
        rootBlockFP = (new ByteArrayDataInput(rootCode.bytes, rootCode.offset, rootCode.length)).readVLong()
            >>> Lucene40BlockTreeTermsReader.OUTPUT_FLAGS_NUM_BITS;
        // Initialize FST always off-heap.
        final IndexInput clone = indexIn.clone();
        clone.seek(indexStartFP);
        if (metaIn == indexIn) { // Only true before Lucene 8.6
            index = new FST<>(clone, clone, ByteSequenceOutputs.getSingleton(), new OffHeapFSTStore());
        } else {
            index = new FST<>(metaIn, clone, ByteSequenceOutputs.getSingleton(), new OffHeapFSTStore());
        }
        /*
         if (false) {
         final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
         Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
         Util.toDot(index, w, false, false);
         System.out.println("FST INDEX: SAVED to " + dotFileName);
         w.close();
         }
        */
    }

    @Override
    public BytesRef getMin() throws IOException {
        if (minTerm == null) {
            // Older index that didn't store min/maxTerm
            return super.getMin();
        } else {
            return minTerm;
        }
    }

    @Override
    public BytesRef getMax() throws IOException {
        if (maxTerm == null) {
            // Older index that didn't store min/maxTerm
            return super.getMax();
        } else {
            return maxTerm;
        }
    }

    /** For debugging -- used by CheckIndex too */
    @Override
    public Stats getStats() throws IOException {
        return new SegmentTermsEnum(this).computeBlockStats();
    }

    @Override
    public boolean hasFreqs() {
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    public boolean hasPositions() {
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
        return fieldInfo.hasPayloads();
    }

    @Override
    public TermsEnum iterator() throws IOException {
        return new SegmentTermsEnum(this);
    }

    @Override
    public long size() {
        return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
        return sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() {
        return sumDocFreq;
    }

    @Override
    public int getDocCount() {
        return docCount;
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
        // if (DEBUG) System.out.println(" FieldReader.intersect startTerm=" +
        // BlockTreeTermsWriter.brToString(startTerm));
        // System.out.println("intersect: " + compiled.type + " a=" + compiled.automaton);
        // TODO: we could push "it's a range" or "it's a prefix" down into IntersectTermsEnum?
        // can we optimize knowing that...?
        if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
            throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
        }
        return new IntersectTermsEnum(this, compiled.automaton, compiled.runAutomaton, compiled.commonSuffixRef, startTerm);
    }

    @Override
    public String toString() {
        return "BlockTreeTerms(seg="
            + parent.segment
            + " terms="
            + numTerms
            + ",postings="
            + sumDocFreq
            + ",positions="
            + sumTotalTermFreq
            + ",docs="
            + docCount
            + ")";
    }
}
