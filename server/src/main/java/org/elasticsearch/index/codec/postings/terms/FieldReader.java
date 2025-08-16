/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.postings.terms;

import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;

import java.io.IOException;

import static org.elasticsearch.index.codec.postings.terms.Lucene90BlockTreeTermsReader.VERSION_MSB_VLONG_OUTPUT;

/**
 * BlockTree's implementation of {@link Terms}.
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
    final Lucene90BlockTreeTermsReader parent;

    final FST<BytesRef> index;

    // private boolean DEBUG;

    FieldReader(
        Lucene90BlockTreeTermsReader parent,
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
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        // if (DEBUG) {
        // System.out.println("BTTR: seg=" + segment + " field=" + fieldInfo.name + " rootBlockCode="
        // + rootCode + " divisor=" + indexDivisor);
        // }
        rootBlockFP = readVLongOutput(new ByteArrayDataInput(rootCode.bytes, rootCode.offset, rootCode.length))
            >>> Lucene90BlockTreeTermsReader.OUTPUT_FLAGS_NUM_BITS;
        // Initialize FST always off-heap.
        var metadata = FST.readMetadata(metaIn, ByteSequenceOutputs.getSingleton());
        index = FST.fromFSTReader(metadata, new OffHeapFSTStore(indexIn, indexStartFP, metadata));
        /*
         if (false) {
         final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
         Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
         Util.toDot(index, w, false, false);
         System.out.println("FST INDEX: SAVED to " + dotFileName);
         w.close();
         }
        */
        BytesRef emptyOutput = metadata.getEmptyOutput();
        if (rootCode.equals(emptyOutput) == false) {
            // TODO: this branch is never taken
            assert false;
            this.rootCode = rootCode;
        } else {
            this.rootCode = emptyOutput;
        }
    }

    long readVLongOutput(DataInput in) throws IOException {
        if (parent.version >= VERSION_MSB_VLONG_OUTPUT) {
            return readMSBVLong(in);
        } else {
            return in.readVLong();
        }
    }

    /**
     * Decodes a variable length byte[] in MSB order back to long, as written by {@link
     * Lucene90BlockTreeTermsWriter#writeMSBVLong}.
     *
     * <p>Package private for testing.
     */
    static long readMSBVLong(DataInput in) throws IOException {
        long l = 0L;
        while (true) {
            byte b = in.readByte();
            l = (l << 7) | (b & 0x7FL);
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return l;
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
        // ToStringUtils.bytesRefToString(startTerm));
        // System.out.println("intersect: " + compiled.type + " a=" + compiled.automaton);
        // TODO: we could push "it's a range" or "it's a prefix" down into IntersectTermsEnum?
        // can we optimize knowing that...?
        if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
            throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
        }
        return new IntersectTermsEnum(
            this,
            compiled.getTransitionAccessor(),
            compiled.getByteRunnable(),
            compiled.commonSuffixRef,
            startTerm
        );
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

    // CHANGES:

    public BytesRef getMinTerm() {
        return minTerm;
    }

    public BytesRef getMaxTerm() {
        return maxTerm;
    }

    // END CHANGES:
}
