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
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

// TODO: break this into WritableFST and ReadOnlyFST.. then
// we can have subclasses of ReadOnlyFST to handle the
// different byte[] level encodings (packed or
// not)... and things like nodeCount, arcCount are read only

// TODO: if FST is pure prefix trie we can do a more compact
// job, ie, once we are at a 'suffix only', just store the
// completion labels as a string not as a series of arcs.

// NOTE: while the FST is able to represent a non-final
// dead-end state (NON_FINAL_END_NODE=0), the layers above
// (FSTEnum, Util) have problems with this!!

/**
 * Represents an finite state machine (FST), using a compact byte[] format.
 *
 * <p>The format is similar to what's used by Morfologik
 * (https://github.com/morfologik/morfologik-stemming).
 *
 * <p>See the {@link org.apache.lucene.util.fst package documentation} for some simple examples.
 *
 */
public final class FST<T> implements Accountable {

    /** Specifies allowed range of each int input label for this FST. */
    public enum INPUT_TYPE {
        BYTE1,
        BYTE2,
        BYTE4
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FST.class);

    private static final int BIT_FINAL_ARC = 1 << 0;
    static final int BIT_LAST_ARC = 1 << 1;
    static final int BIT_TARGET_NEXT = 1 << 2;

    // TODO: we can free up a bit if we can nuke this:
    private static final int BIT_STOP_NODE = 1 << 3;

    /** This flag is set if the arc has an output. */
    public static final int BIT_ARC_HAS_OUTPUT = 1 << 4;

    private static final int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

    /** Value of the arc flags to declare a node with fixed length arcs designed for binary search. */
    // We use this as a marker because this one flag is illegal by itself.
    public static final byte ARCS_FOR_BINARY_SEARCH = BIT_ARC_HAS_FINAL_OUTPUT;

    /**
     * Value of the arc flags to declare a node with fixed length arcs and bit table designed for
     * direct addressing.
     */
    static final byte ARCS_FOR_DIRECT_ADDRESSING = 1 << 6;

    /** @see #shouldExpandNodeWithFixedLengthArcs */
    static final int FIXED_LENGTH_ARC_SHALLOW_DEPTH = 3; // 0 => only root node.

    /** @see #shouldExpandNodeWithFixedLengthArcs */
    static final int FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS = 5;

    /** @see #shouldExpandNodeWithFixedLengthArcs */
    static final int FIXED_LENGTH_ARC_DEEP_NUM_ARCS = 10;

    /**
     * Maximum oversizing factor allowed for direct addressing compared to binary search when
     * expansion credits allow the oversizing. This factor prevents expansions that are obviously too
     * costly even if there are sufficient credits.
     *
     * @see #shouldExpandNodeWithDirectAddressing
     */
    private static final float DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR = 1.66f;

    // Increment version to change it
    private static final String FILE_FORMAT_NAME = "FST";
    private static final int VERSION_START = 0;
    /** Changed numBytesPerArc for array'd case from byte to int. */
    private static final int VERSION_INT_NUM_BYTES_PER_ARC = 1;

    /** Write BYTE2 labels as 2-byte short, not vInt. */
    private static final int VERSION_SHORT_BYTE2_LABELS = 2;

    /** Added optional packed format. */
    private static final int VERSION_PACKED = 3;

    /** Changed from int to vInt for encoding arc targets.
     *  Also changed maxBytesPerArc from int to vInt in the array case. */
    private static final int VERSION_VINT_TARGET = 4;

    /** Don't store arcWithOutputCount anymore */
    private static final int VERSION_NO_NODE_ARC_COUNTS = 5;

    private static final int VERSION_PACKED_REMOVED = 6;

    private static final int VERSION_LITTLE_ENDIAN = 8;
    private static final int VERSION_CURRENT = VERSION_LITTLE_ENDIAN;

    // Never serialized; just used to represent the virtual
    // final node w/ no arcs:
    private static final long FINAL_END_NODE = -1;

    // Never serialized; just used to represent the virtual
    // non-final node w/ no arcs:
    private static final long NON_FINAL_END_NODE = 0;

    /** If arc has this label then that arc is final/accepted */
    public static final int END_LABEL = -1;

    final INPUT_TYPE inputType;

    // if non-null, this FST accepts the empty string and
    // produces this output
    T emptyOutput;

    /**
     * A {@link BytesStore}, used during building, or during reading when the FST is very large (more
     * than 1 GB). If the FST is less than 1 GB then bytesArray is set instead.
     */
    final BytesStore bytes;

    private final FSTStore fstStore;

    private long startNode = -1;

    public final Outputs<T> outputs;

    private final int version;

    /** Represents a single arc. */
    public static final class Arc<T> {

        // *** Arc fields.

        private int label;

        private T output;

        private long target;

        private byte flags;

        private T nextFinalOutput;

        private long nextArc;

        private byte nodeFlags;

        // *** Fields for arcs belonging to a node with fixed length arcs.
        // So only valid when bytesPerArc != 0.
        // nodeFlags == ARCS_FOR_BINARY_SEARCH || nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

        private int bytesPerArc;

        private long posArcsStart;

        private int arcIdx;

        private int numArcs;

        // *** Fields for a direct addressing node. nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

        /**
         * Start position in the {@link BytesReader} of the presence bits for a direct addressing
         * node, aka the bit-table
         */
        private long bitTableStart;

        /** First label of a direct addressing node. */
        private int firstLabel;

        /**
         * Index of the current label of a direct addressing node. While {@link #arcIdx} is the current
         * index in the label range, {@link #presenceIndex} is its corresponding index in the list of
         * actually present labels. It is equal to the number of bits set before the bit at {@link
         * #arcIdx} in the bit-table. This field is a cache to avoid to count bits set repeatedly when
         * iterating the next arcs.
         */
        private int presenceIndex;

        /** Returns this */
        public Arc<T> copyFrom(Arc<T> other) {
            label = other.label();
            target = other.target();
            flags = other.flags();
            output = other.output();
            nextFinalOutput = other.nextFinalOutput();
            nextArc = other.nextArc();
            nodeFlags = other.nodeFlags();
            bytesPerArc = other.bytesPerArc();

            // Fields for arcs belonging to a node with fixed length arcs.
            // We could avoid copying them if bytesPerArc() == 0 (this was the case with previous code,
            // and the current code
            // still supports that), but it may actually help external uses of FST to have consistent arc
            // state, and debugging
            // is easier.
            posArcsStart = other.posArcsStart();
            arcIdx = other.arcIdx();
            numArcs = other.numArcs();
            bitTableStart = other.bitTableStart;
            firstLabel = other.firstLabel();
            presenceIndex = other.presenceIndex;

            return this;
        }

        boolean flag(int flag) {
            return FST.flag(flags, flag);
        }

        public boolean isLast() {
            return flag(BIT_LAST_ARC);
        }

        public boolean isFinal() {
            return flag(BIT_FINAL_ARC);
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append(" target=").append(target());
            b.append(" label=0x").append(Integer.toHexString(label()));
            if (flag(BIT_FINAL_ARC)) {
                b.append(" final");
            }
            if (flag(BIT_LAST_ARC)) {
                b.append(" last");
            }
            if (flag(BIT_TARGET_NEXT)) {
                b.append(" targetNext");
            }
            if (flag(BIT_STOP_NODE)) {
                b.append(" stop");
            }
            if (flag(BIT_ARC_HAS_OUTPUT)) {
                b.append(" output=").append(output());
            }
            if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
                b.append(" nextFinalOutput=").append(nextFinalOutput());
            }
            if (bytesPerArc() != 0) {
                b.append(" arcArray(idx=")
                    .append(arcIdx())
                    .append(" of ")
                    .append(numArcs())
                    .append(")")
                    .append("(")
                    .append(nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING ? "da" : "bs")
                    .append(")");
            }
            return b.toString();
        }

        public int label() {
            return label;
        }

        public T output() {
            return output;
        }

        /** Ord/address to target node. */
        public long target() {
            return target;
        }

        public byte flags() {
            return flags;
        }

        public T nextFinalOutput() {
            return nextFinalOutput;
        }

        /**
         * Address (into the byte[]) of the next arc - only for list of variable length arc. Or
         * ord/address to the next node if label == {@link #END_LABEL}.
         */
        long nextArc() {
            return nextArc;
        }

        /** Where we are in the array; only valid if bytesPerArc != 0. */
        public int arcIdx() {
            return arcIdx;
        }

        /**
         * Node header flags. Only meaningful to check if the value is either {@link
         * #ARCS_FOR_BINARY_SEARCH} or {@link #ARCS_FOR_DIRECT_ADDRESSING} (other value when bytesPerArc
         * == 0).
         */
        public byte nodeFlags() {
            return nodeFlags;
        }

        /** Where the first arc in the array starts; only valid if bytesPerArc != 0 */
        public long posArcsStart() {
            return posArcsStart;
        }

        /**
         * Non-zero if this arc is part of a node with fixed length arcs, which means all arcs for the
         * node are encoded with a fixed number of bytes so that we binary search or direct address. We
         * do when there are enough arcs leaving one node. It wastes some bytes but gives faster
         * lookups.
         */
        public int bytesPerArc() {
            return bytesPerArc;
        }

        /**
         * How many arcs; only valid if bytesPerArc != 0 (fixed length arcs). For a node designed for
         * binary search this is the array size. For a node designed for direct addressing, this is the
         * label range.
         */
        public int numArcs() {
            return numArcs;
        }

        /**
         * First label of a direct addressing node. Only valid if nodeFlags == {@link
         * #ARCS_FOR_DIRECT_ADDRESSING}.
         */
        int firstLabel() {
            return firstLabel;
        }

        /**
         * Helper methods to read the bit-table of a direct addressing node. Only valid for {@link Arc}
         * with {@link Arc#nodeFlags()} == {@code ARCS_FOR_DIRECT_ADDRESSING}.
         */
        static class BitTable {

            /** See {@link BitTableUtil#isBitSet(int, BytesReader)}. */
            static boolean isBitSet(int bitIndex, Arc<?> arc, BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.isBitSet(bitIndex, in);
            }

            /**
             * See {@link BitTableUtil#countBits(int, BytesReader)}. The count of bit set is the
             * number of arcs of a direct addressing node.
             */
            static int countBits(Arc<?> arc, BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.countBits(getNumPresenceBytes(arc.numArcs()), in);
            }

            /** See {@link BitTableUtil#countBitsUpTo(int, BytesReader)}. */
            static int countBitsUpTo(int bitIndex, Arc<?> arc, BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.countBitsUpTo(bitIndex, in);
            }

            /** See {@link BitTableUtil#nextBitSet(int, int, BytesReader)}. */
            static int nextBitSet(int bitIndex, Arc<?> arc, BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.nextBitSet(bitIndex, getNumPresenceBytes(arc.numArcs()), in);
            }

            /** See {@link BitTableUtil#previousBitSet(int, BytesReader)}. */
            static int previousBitSet(int bitIndex, Arc<?> arc, BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.previousBitSet(bitIndex, in);
            }

            /** Asserts the bit-table of the provided {@link Arc} is valid. */
            static boolean assertIsValid(Arc<?> arc, BytesReader in) throws IOException {
                assert arc.bytesPerArc() > 0;
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                // First bit must be set.
                assert isBitSet(0, arc, in);
                // Last bit must be set.
                assert isBitSet(arc.numArcs() - 1, arc, in);
                // No bit set after the last arc.
                assert nextBitSet(arc.numArcs() - 1, arc, in) == -1;
                return true;
            }
        }
    }

    private static boolean flag(int flags, int bit) {
        return (flags & bit) != 0;
    }

    // make a new empty FST, for building; Builder invokes this
    FST(INPUT_TYPE inputType, Outputs<T> outputs, int bytesPageBits) {
        this.inputType = inputType;
        this.outputs = outputs;
        fstStore = null;
        bytes = new BytesStore(bytesPageBits);
        // pad: ensure no node gets address 0 which is reserved to mean
        // the stop state w/ no arcs
        bytes.writeByte((byte) 0);
        emptyOutput = null;
        this.version = VERSION_CURRENT;
    }

    private static final int DEFAULT_MAX_BLOCK_BITS = Constants.JRE_IS_64BIT ? 30 : 28;

    /** Load a previously saved FST. */
    public FST(DataInput metaIn, DataInput in, Outputs<T> outputs) throws IOException {
        this(metaIn, in, outputs, new OnHeapFSTStore(DEFAULT_MAX_BLOCK_BITS));
    }

    /**
     * Load a previously saved FST; maxBlockBits allows you to control the size of the byte[] pages
     * used to hold the FST bytes.
     */
    public FST(DataInput metaIn, DataInput in, Outputs<T> outputs, FSTStore fstStore) throws IOException {
        bytes = null;
        this.fstStore = fstStore;
        this.outputs = outputs;

        // NOTE: only reads formats VERSION_START up to VERSION_CURRENT; we don't have
        // back-compat promise for FSTs (they are experimental), but we are sometimes able to offer it
        this.version = CodecUtil.checkHeader(metaIn, FILE_FORMAT_NAME, VERSION_START, VERSION_CURRENT);
        if (version < VERSION_PACKED_REMOVED) {
            if (in.readByte() == 1) {
                throw new CorruptIndexException("Cannot read packed FSTs anymore", in);
            }
        }
        if (metaIn.readByte() == 1) {
            // accepts empty string
            // 1 KB blocks:
            BytesStore emptyBytes = new BytesStore(10);
            int numBytes = metaIn.readVInt();
            emptyBytes.copyBytes(metaIn, numBytes);

            // De-serialize empty-string output:
            BytesReader reader = emptyBytes.getReverseReader();
            // NoOutputs uses 0 bytes when writing its output,
            // so we have to check here else BytesStore gets
            // angry:
            if (numBytes > 0) {
                reader.setPosition(numBytes - 1);
            }
            emptyOutput = outputs.readFinalOutput(reader);
        } else {
            emptyOutput = null;
        }
        final byte t = metaIn.readByte();
        switch (t) {
            case 0:
                inputType = INPUT_TYPE.BYTE1;
                break;
            case 1:
                inputType = INPUT_TYPE.BYTE2;
                break;
            case 2:
                inputType = INPUT_TYPE.BYTE4;
                break;
            default:
                throw new CorruptIndexException("invalid input type " + t, in);
        }
        startNode = metaIn.readVLong();
        if (version < VERSION_NO_NODE_ARC_COUNTS) {
            metaIn.readVLong();
            metaIn.readVLong();
            metaIn.readVLong();
        }

        long numBytes = metaIn.readVLong();
        this.fstStore.init(in, numBytes);
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_RAM_BYTES_USED;
        if (this.fstStore != null) {
            size += this.fstStore.ramBytesUsed();
        } else {
            size += bytes.ramBytesUsed();
        }

        return size;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(input=" + inputType + ",output=" + outputs;
    }

    void finish(long newStartNode) throws IOException {
        assert newStartNode <= bytes.getPosition();
        if (startNode != -1) {
            throw new IllegalStateException("already finished");
        }
        if (newStartNode == FINAL_END_NODE && emptyOutput != null) {
            newStartNode = 0;
        }
        startNode = newStartNode;
        bytes.finish();
    }

    public T getEmptyOutput() {
        return emptyOutput;
    }

    void setEmptyOutput(T v) {
        if (emptyOutput != null) {
            emptyOutput = outputs.merge(emptyOutput, v);
        } else {
            emptyOutput = v;
        }
    }

    public void save(DataOutput metaOut, DataOutput out) throws IOException {
        if (startNode == -1) {
            throw new IllegalStateException("call finish first");
        }
        CodecUtil.writeHeader(metaOut, FILE_FORMAT_NAME, VERSION_CURRENT);
        // TODO: really we should encode this as an arc, arriving
        // to the root node, instead of special casing here:
        if (emptyOutput != null) {
            // Accepts empty string
            metaOut.writeByte((byte) 1);

            // Serialize empty-string output:
            ByteBuffersDataOutput ros = new ByteBuffersDataOutput();
            outputs.writeFinalOutput(emptyOutput, ros);
            byte[] emptyOutputBytes = ros.toArrayCopy();
            int emptyLen = emptyOutputBytes.length;

            // reverse
            final int stopAt = emptyLen / 2;
            int upto = 0;
            while (upto < stopAt) {
                final byte b = emptyOutputBytes[upto];
                emptyOutputBytes[upto] = emptyOutputBytes[emptyLen - upto - 1];
                emptyOutputBytes[emptyLen - upto - 1] = b;
                upto++;
            }
            metaOut.writeVInt(emptyLen);
            metaOut.writeBytes(emptyOutputBytes, 0, emptyLen);
        } else {
            metaOut.writeByte((byte) 0);
        }
        final byte t;
        if (inputType == FST.INPUT_TYPE.BYTE1) {
            t = 0;
        } else if (inputType == FST.INPUT_TYPE.BYTE2) {
            t = 1;
        } else {
            t = 2;
        }
        metaOut.writeByte(t);
        metaOut.writeVLong(startNode);
        if (bytes != null) {
            long numBytes = bytes.getPosition();
            metaOut.writeVLong(numBytes);
            bytes.writeTo(out);
        } else {
            assert fstStore != null;
            fstStore.writeTo(out);
        }
    }

    /** Writes an automaton to a file. */
    public void save(final Path path) throws IOException {
        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
            DataOutput out = new OutputStreamDataOutput(os);
            save(out, out);
        }
    }

    /** Reads an automaton from a file. */
    public static <T> FST<T> read(Path path, Outputs<T> outputs) throws IOException {
        try (InputStream is = Files.newInputStream(path)) {
            DataInput in = new InputStreamDataInput(new BufferedInputStream(is));
            return new FST<>(in, in, outputs);
        }
    }

    private void writeLabel(DataOutput out, int v) throws IOException {
        assert v >= 0 : "v=" + v;
        if (inputType == FST.INPUT_TYPE.BYTE1) {
            assert v <= 255 : "v=" + v;
            out.writeByte((byte) v);
        } else if (inputType == FST.INPUT_TYPE.BYTE2) {
            assert v <= 65535 : "v=" + v;
            out.writeShort((short) v);
        } else {
            out.writeVInt(v);
        }
    }

    /** Reads one BYTE1/2/4 label from the provided {@link DataInput}. */
    public int readLabel(DataInput in) throws IOException {
        final int v;
        if (inputType == INPUT_TYPE.BYTE1) {
            // Unsigned byte:
            v = in.readByte() & 0xFF;
        } else if (inputType == INPUT_TYPE.BYTE2) {
            // Unsigned short:
            if (version < VERSION_LITTLE_ENDIAN) {
                v = Short.reverseBytes(in.readShort()) & 0xFFFF;
            } else {
                v = in.readShort() & 0xFFFF;
            }
        } else {
            v = in.readVInt();
        }
        return v;
    }

    /** returns true if the node at this address has any outgoing arcs */
    public static <T> boolean targetHasArcs(Arc<T> arc) {
        return arc.target() > 0;
    }

    // serializes new node by appending its bytes to the end
    // of the current byte[]
    long addNode(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn) throws IOException {
        T NO_OUTPUT = outputs.getNoOutput();

        // System.out.println("FST.addNode pos=" + bytes.getPosition() + " numArcs=" + nodeIn.numArcs);
        if (nodeIn.numArcs == 0) {
            if (nodeIn.isFinal) {
                return FINAL_END_NODE;
            } else {
                return NON_FINAL_END_NODE;
            }
        }
        final long startAddress = fstCompiler.bytes.getPosition();
        // System.out.println(" startAddr=" + startAddress);

        final boolean doFixedLengthArcs = shouldExpandNodeWithFixedLengthArcs(fstCompiler, nodeIn);
        if (doFixedLengthArcs) {
            // System.out.println(" fixed length arcs");
            if (fstCompiler.numBytesPerArc.length < nodeIn.numArcs) {
                fstCompiler.numBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, Integer.BYTES)];
                fstCompiler.numLabelBytesPerArc = new int[fstCompiler.numBytesPerArc.length];
            }
        }

        fstCompiler.arcCount += nodeIn.numArcs;

        final int lastArc = nodeIn.numArcs - 1;

        long lastArcStart = fstCompiler.bytes.getPosition();
        int maxBytesPerArc = 0;
        int maxBytesPerArcWithoutLabel = 0;
        for (int arcIdx = 0; arcIdx < nodeIn.numArcs; arcIdx++) {
            final FSTCompiler.Arc<T> arc = nodeIn.arcs[arcIdx];
            final FSTCompiler.CompiledNode target = (FSTCompiler.CompiledNode) arc.target;
            int flags = 0;
            // System.out.println(" arc " + arcIdx + " label=" + arc.label + " -> target=" +
            // target.node);

            if (arcIdx == lastArc) {
                flags += BIT_LAST_ARC;
            }

            if (fstCompiler.lastFrozenNode == target.node && doFixedLengthArcs == false) {
                // TODO: for better perf (but more RAM used) we
                // could avoid this except when arc is "near" the
                // last arc:
                flags += BIT_TARGET_NEXT;
            }

            if (arc.isFinal) {
                flags += BIT_FINAL_ARC;
                if (arc.nextFinalOutput != NO_OUTPUT) {
                    flags += BIT_ARC_HAS_FINAL_OUTPUT;
                }
            } else {
                assert arc.nextFinalOutput == NO_OUTPUT;
            }

            boolean targetHasArcs = target.node > 0;

            if (targetHasArcs == false) {
                flags += BIT_STOP_NODE;
            }

            if (arc.output != NO_OUTPUT) {
                flags += BIT_ARC_HAS_OUTPUT;
            }

            fstCompiler.bytes.writeByte((byte) flags);
            long labelStart = fstCompiler.bytes.getPosition();
            writeLabel(fstCompiler.bytes, arc.label);
            int numLabelBytes = (int) (fstCompiler.bytes.getPosition() - labelStart);

            // System.out.println(" write arc: label=" + (char) arc.label + " flags=" + flags + "
            // target=" + target.node + " pos=" + bytes.getPosition() + " output=" +
            // outputs.outputToString(arc.output));

            if (arc.output != NO_OUTPUT) {
                outputs.write(arc.output, fstCompiler.bytes);
                // System.out.println(" write output");
            }

            if (arc.nextFinalOutput != NO_OUTPUT) {
                // System.out.println(" write final output");
                outputs.writeFinalOutput(arc.nextFinalOutput, fstCompiler.bytes);
            }

            if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
                assert target.node > 0;
                // System.out.println(" write target");
                fstCompiler.bytes.writeVLong(target.node);
            }

            // just write the arcs "like normal" on first pass, but record how many bytes each one took
            // and max byte size:
            if (doFixedLengthArcs) {
                int numArcBytes = (int) (fstCompiler.bytes.getPosition() - lastArcStart);
                fstCompiler.numBytesPerArc[arcIdx] = numArcBytes;
                fstCompiler.numLabelBytesPerArc[arcIdx] = numLabelBytes;
                lastArcStart = fstCompiler.bytes.getPosition();
                maxBytesPerArc = Math.max(maxBytesPerArc, numArcBytes);
                maxBytesPerArcWithoutLabel = Math.max(maxBytesPerArcWithoutLabel, numArcBytes - numLabelBytes);
                // System.out.println(" arcBytes=" + numArcBytes + " labelBytes=" + numLabelBytes);
            }
        }

        // TODO: try to avoid wasteful cases: disable doFixedLengthArcs in that case
        /*
         *
         * LUCENE-4682: what is a fair heuristic here?
         * It could involve some of these:
         * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
         * 2. how much binSearch saves over scan: nodeIn.numArcs
         * 3. waste: numBytes vs numBytesExpanded
         *
         * the one below just looks at #3
        if (doFixedLengthArcs) {
          // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
          int numBytes = lastArcStart - startAddress;
          int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
          if (numBytesExpanded > numBytes*1.25) {
        doFixedLengthArcs = false;
          }
        }
        */

        if (doFixedLengthArcs) {
            assert maxBytesPerArc > 0;
            // 2nd pass just "expands" all arcs to take up a fixed byte size

            int labelRange = nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label + 1;
            assert labelRange > 0;
            if (shouldExpandNodeWithDirectAddressing(fstCompiler, nodeIn, maxBytesPerArc, maxBytesPerArcWithoutLabel, labelRange)) {
                writeNodeForDirectAddressing(fstCompiler, nodeIn, startAddress, maxBytesPerArcWithoutLabel, labelRange);
                fstCompiler.directAddressingNodeCount++;
            } else {
                writeNodeForBinarySearch(fstCompiler, nodeIn, startAddress, maxBytesPerArc);
                fstCompiler.binarySearchNodeCount++;
            }
        }

        final long thisNodeAddress = fstCompiler.bytes.getPosition() - 1;
        fstCompiler.bytes.reverse(startAddress, thisNodeAddress);
        fstCompiler.nodeCount++;
        return thisNodeAddress;
    }

    /**
     * Returns whether the given node should be expanded with fixed length arcs. Nodes will be
     * expanded depending on their depth (distance from the root node) and their number of arcs.
     *
     * <p>Nodes with fixed length arcs use more space, because they encode all arcs with a fixed
     * number of bytes, but they allow either binary search or direct addressing on the arcs (instead
     * of linear scan) on lookup by arc label.
     */
    private boolean shouldExpandNodeWithFixedLengthArcs(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> node) {
        return fstCompiler.allowFixedLengthArcs
            && ((node.depth <= FIXED_LENGTH_ARC_SHALLOW_DEPTH && node.numArcs >= FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS)
                || node.numArcs >= FIXED_LENGTH_ARC_DEEP_NUM_ARCS);
    }

    /**
     * Returns whether the given node should be expanded with direct addressing instead of binary
     * search.
     *
     * <p>Prefer direct addressing for performance if it does not oversize binary search byte size too
     * much, so that the arcs can be directly addressed by label.
     *
     * @see FSTCompiler#getDirectAddressingMaxOversizingFactor()
     */
    private boolean shouldExpandNodeWithDirectAddressing(
        FSTCompiler<T> fstCompiler,
        FSTCompiler.UnCompiledNode<T> nodeIn,
        int numBytesPerArc,
        int maxBytesPerArcWithoutLabel,
        int labelRange
    ) {
        // Anticipate precisely the size of the encodings.
        int sizeForBinarySearch = numBytesPerArc * nodeIn.numArcs;
        int sizeForDirectAddressing = getNumPresenceBytes(labelRange) + fstCompiler.numLabelBytesPerArc[0] + maxBytesPerArcWithoutLabel
            * nodeIn.numArcs;

        // Determine the allowed oversize compared to binary search.
        // This is defined by a parameter of FST Builder (default 1: no oversize).
        int allowedOversize = (int) (sizeForBinarySearch * fstCompiler.getDirectAddressingMaxOversizingFactor());
        int expansionCost = sizeForDirectAddressing - allowedOversize;

        // Select direct addressing if either:
        // - Direct addressing size is smaller than binary search.
        // In this case, increment the credit by the reduced size (to use it later).
        // - Direct addressing size is larger than binary search, but the positive credit allows the
        // oversizing.
        // In this case, decrement the credit by the oversize.
        // In addition, do not try to oversize to a clearly too large node size
        // (this is the DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR parameter).
        if (expansionCost <= 0
            || (fstCompiler.directAddressingExpansionCredit >= expansionCost
                && sizeForDirectAddressing <= allowedOversize * DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR)) {
            fstCompiler.directAddressingExpansionCredit -= expansionCost;
            return true;
        }
        return false;
    }

    private void writeNodeForBinarySearch(
        FSTCompiler<T> fstCompiler,
        FSTCompiler.UnCompiledNode<T> nodeIn,
        long startAddress,
        int maxBytesPerArc
    ) {
        // Build the header in a buffer.
        // It is a false/special arc which is in fact a node header with node flags followed by node
        // metadata.
        fstCompiler.fixedLengthArcsBuffer.resetPosition()
            .writeByte(ARCS_FOR_BINARY_SEARCH)
            .writeVInt(nodeIn.numArcs)
            .writeVInt(maxBytesPerArc);
        int headerLen = fstCompiler.fixedLengthArcsBuffer.getPosition();

        // Expand the arcs in place, backwards.
        long srcPos = fstCompiler.bytes.getPosition();
        long destPos = startAddress + headerLen + nodeIn.numArcs * maxBytesPerArc;
        assert destPos >= srcPos;
        if (destPos > srcPos) {
            fstCompiler.bytes.skipBytes((int) (destPos - srcPos));
            for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
                destPos -= maxBytesPerArc;
                int arcLen = fstCompiler.numBytesPerArc[arcIdx];
                srcPos -= arcLen;
                if (srcPos != destPos) {
                    assert destPos > srcPos
                        : "destPos="
                            + destPos
                            + " srcPos="
                            + srcPos
                            + " arcIdx="
                            + arcIdx
                            + " maxBytesPerArc="
                            + maxBytesPerArc
                            + " arcLen="
                            + arcLen
                            + " nodeIn.numArcs="
                            + nodeIn.numArcs;
                    fstCompiler.bytes.copyBytes(srcPos, destPos, arcLen);
                }
            }
        }

        // Write the header.
        fstCompiler.bytes.writeBytes(startAddress, fstCompiler.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
    }

    private void writeNodeForDirectAddressing(
        FSTCompiler<T> fstCompiler,
        FSTCompiler.UnCompiledNode<T> nodeIn,
        long startAddress,
        int maxBytesPerArcWithoutLabel,
        int labelRange
    ) {
        // Expand the arcs backwards in a buffer because we remove the labels.
        // So the obtained arcs might occupy less space. This is the reason why this
        // whole method is more complex.
        // Drop the label bytes since we can infer the label based on the arc index,
        // the presence bits, and the first label. Keep the first label.
        int headerMaxLen = 11;
        int numPresenceBytes = getNumPresenceBytes(labelRange);
        long srcPos = fstCompiler.bytes.getPosition();
        int totalArcBytes = fstCompiler.numLabelBytesPerArc[0] + nodeIn.numArcs * maxBytesPerArcWithoutLabel;
        int bufferOffset = headerMaxLen + numPresenceBytes + totalArcBytes;
        byte[] buffer = fstCompiler.fixedLengthArcsBuffer.ensureCapacity(bufferOffset).getBytes();
        // Copy the arcs to the buffer, dropping all labels except first one.
        for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
            bufferOffset -= maxBytesPerArcWithoutLabel;
            int srcArcLen = fstCompiler.numBytesPerArc[arcIdx];
            srcPos -= srcArcLen;
            int labelLen = fstCompiler.numLabelBytesPerArc[arcIdx];
            // Copy the flags.
            fstCompiler.bytes.copyBytes(srcPos, buffer, bufferOffset, 1);
            // Skip the label, copy the remaining.
            int remainingArcLen = srcArcLen - 1 - labelLen;
            if (remainingArcLen != 0) {
                fstCompiler.bytes.copyBytes(srcPos + 1 + labelLen, buffer, bufferOffset + 1, remainingArcLen);
            }
            if (arcIdx == 0) {
                // Copy the label of the first arc only.
                bufferOffset -= labelLen;
                fstCompiler.bytes.copyBytes(srcPos + 1, buffer, bufferOffset, labelLen);
            }
        }
        assert bufferOffset == headerMaxLen + numPresenceBytes;

        // Build the header in the buffer.
        // It is a false/special arc which is in fact a node header with node flags followed by node
        // metadata.
        fstCompiler.fixedLengthArcsBuffer.resetPosition()
            .writeByte(ARCS_FOR_DIRECT_ADDRESSING)
            .writeVInt(labelRange) // labelRange instead of numArcs.
            .writeVInt(maxBytesPerArcWithoutLabel); // maxBytesPerArcWithoutLabel instead of maxBytesPerArc.
        int headerLen = fstCompiler.fixedLengthArcsBuffer.getPosition();

        // Prepare the builder byte store. Enlarge or truncate if needed.
        long nodeEnd = startAddress + headerLen + numPresenceBytes + totalArcBytes;
        long currentPosition = fstCompiler.bytes.getPosition();
        if (nodeEnd >= currentPosition) {
            fstCompiler.bytes.skipBytes((int) (nodeEnd - currentPosition));
        } else {
            fstCompiler.bytes.truncate(nodeEnd);
        }
        assert fstCompiler.bytes.getPosition() == nodeEnd;

        // Write the header.
        long writeOffset = startAddress;
        fstCompiler.bytes.writeBytes(writeOffset, fstCompiler.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
        writeOffset += headerLen;

        // Write the presence bits
        writePresenceBits(fstCompiler, nodeIn, writeOffset, numPresenceBytes);
        writeOffset += numPresenceBytes;

        // Write the first label and the arcs.
        fstCompiler.bytes.writeBytes(writeOffset, fstCompiler.fixedLengthArcsBuffer.getBytes(), bufferOffset, totalArcBytes);
    }

    private void writePresenceBits(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn, long dest, int numPresenceBytes) {
        long bytePos = dest;
        byte presenceBits = 1; // The first arc is always present.
        int presenceIndex = 0;
        int previousLabel = nodeIn.arcs[0].label;
        for (int arcIdx = 1; arcIdx < nodeIn.numArcs; arcIdx++) {
            int label = nodeIn.arcs[arcIdx].label;
            assert label > previousLabel;
            presenceIndex += label - previousLabel;
            while (presenceIndex >= Byte.SIZE) {
                fstCompiler.bytes.writeByte(bytePos++, presenceBits);
                presenceBits = 0;
                presenceIndex -= Byte.SIZE;
            }
            // Set the bit at presenceIndex to flag that the corresponding arc is present.
            presenceBits |= (byte) (1 << presenceIndex);
            previousLabel = label;
        }
        assert presenceIndex == (nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label) % 8;
        assert presenceBits != 0; // The last byte is not 0.
        assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
        fstCompiler.bytes.writeByte(bytePos++, presenceBits);
        assert bytePos - dest == numPresenceBytes;
    }

    /**
     * Gets the number of bytes required to flag the presence of each arc in the given label range,
     * one bit per arc.
     */
    private static int getNumPresenceBytes(int labelRange) {
        assert labelRange >= 0;
        return (labelRange + 7) >> 3;
    }

    /**
     * Reads the presence bits of a direct-addressing node. Actually we don't read them here, we just
     * keep the pointer to the bit-table start and we skip them.
     */
    private void readPresenceBytes(Arc<T> arc, BytesReader in) throws IOException {
        assert arc.bytesPerArc() > 0;
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        arc.bitTableStart = in.getPosition();
        in.skipBytes(getNumPresenceBytes(arc.numArcs()));
    }

    /** Fills virtual 'start' arc, ie, an empty incoming arc to the FST's start node */
    public Arc<T> getFirstArc(Arc<T> arc) {
        T NO_OUTPUT = outputs.getNoOutput();

        if (emptyOutput != null) {
            arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
            arc.nextFinalOutput = emptyOutput;
            if (emptyOutput != NO_OUTPUT) {
                arc.flags = (byte) (arc.flags() | BIT_ARC_HAS_FINAL_OUTPUT);
            }
        } else {
            arc.flags = BIT_LAST_ARC;
            arc.nextFinalOutput = NO_OUTPUT;
        }
        arc.output = NO_OUTPUT;

        // If there are no nodes, ie, the FST only accepts the
        // empty string, then startNode is 0
        arc.target = startNode;
        return arc;
    }

    /**
     * Follows the <code>follow</code> arc and reads the last arc of its target; this changes the
     * provided <code>arc</code> (2nd arg) in-place and returns it.
     *
     * @return Returns the second argument (<code>arc</code>).
     */
    Arc<T> readLastTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
        // System.out.println("readLast");
        if (targetHasArcs(follow) == false) {
            // System.out.println(" end node");
            assert follow.isFinal();
            arc.label = END_LABEL;
            arc.target = FINAL_END_NODE;
            arc.output = follow.nextFinalOutput();
            arc.flags = BIT_LAST_ARC;
            arc.nodeFlags = arc.flags;
            return arc;
        } else {
            in.setPosition(follow.target());
            byte flags = arc.nodeFlags = in.readByte();
            if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
                // Special arc which is actually a node header for fixed length arcs.
                // Jump straight to end to find the last arc.
                arc.numArcs = in.readVInt();
                if (version >= VERSION_VINT_TARGET) {
                    arc.bytesPerArc = in.readVInt();
                } else {
                    arc.bytesPerArc = in.readInt();
                }
                // System.out.println(" array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
                if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
                    readPresenceBytes(arc, in);
                    arc.firstLabel = readLabel(in);
                    arc.posArcsStart = in.getPosition();
                    readLastArcByDirectAddressing(arc, in);
                } else {
                    arc.arcIdx = arc.numArcs() - 2;
                    arc.posArcsStart = in.getPosition();
                    readNextRealArc(arc, in);
                }
            } else {
                arc.flags = flags;
                // non-array: linear scan
                arc.bytesPerArc = 0;
                // System.out.println(" scan");
                while (arc.isLast() == false) {
                    // skip this arc:
                    readLabel(in);
                    if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
                        outputs.skipOutput(in);
                    }
                    if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
                        outputs.skipFinalOutput(in);
                    }
                    if (arc.flag(BIT_STOP_NODE)) {} else if (arc.flag(BIT_TARGET_NEXT)) {} else {
                        readUnpackedNodeTarget(in);
                    }
                    arc.flags = in.readByte();
                }
                // Undo the byte flags we read:
                in.skipBytes(-1);
                arc.nextArc = in.getPosition();
                readNextRealArc(arc, in);
            }
            assert arc.isLast();
            return arc;
        }
    }

    private long readUnpackedNodeTarget(BytesReader in) throws IOException {
        if (version < VERSION_VINT_TARGET) {
            return in.readInt();
        } else {
            return in.readVLong();
        }
    }

    /**
     * Follow the <code>follow</code> arc and read the first arc of its target; this changes the
     * provided <code>arc</code> (2nd arg) in-place and returns it.
     *
     * @return Returns the second argument (<code>arc</code>).
     */
    public Arc<T> readFirstTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
        // int pos = address;
        // System.out.println(" readFirstTarget follow.target=" + follow.target + " isFinal=" +
        // follow.isFinal());
        if (follow.isFinal()) {
            // Insert "fake" final first arc:
            arc.label = END_LABEL;
            arc.output = follow.nextFinalOutput();
            arc.flags = BIT_FINAL_ARC;
            if (follow.target() <= 0) {
                arc.flags |= BIT_LAST_ARC;
            } else {
                // NOTE: nextArc is a node (not an address!) in this case:
                arc.nextArc = follow.target();
            }
            arc.target = FINAL_END_NODE;
            arc.nodeFlags = arc.flags;
            // System.out.println(" insert isFinal; nextArc=" + follow.target + " isLast=" +
            // arc.isLast() + " output=" + outputs.outputToString(arc.output));
            return arc;
        } else {
            return readFirstRealTargetArc(follow.target(), arc, in);
        }
    }

    public Arc<T> readFirstRealTargetArc(long nodeAddress, Arc<T> arc, final BytesReader in) throws IOException {
        in.setPosition(nodeAddress);
        // System.out.println(" flags=" + arc.flags);

        byte flags = arc.nodeFlags = in.readByte();
        if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
            // System.out.println(" fixed length arc");
            // Special arc which is actually a node header for fixed length arcs.
            arc.numArcs = in.readVInt();
            if (version >= VERSION_VINT_TARGET) {
                arc.bytesPerArc = in.readVInt();
            } else {
                arc.bytesPerArc = in.readInt();
            }
            arc.arcIdx = -1;
            if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
                readPresenceBytes(arc, in);
                arc.firstLabel = readLabel(in);
                arc.presenceIndex = -1;
            }
            arc.posArcsStart = in.getPosition();
            // System.out.println(" bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + "
            // arcsStart=" + pos);
        } else {
            arc.nextArc = nodeAddress;
            arc.bytesPerArc = 0;
        }

        return readNextRealArc(arc, in);
    }

    /**
     * Returns whether <code>arc</code>'s target points to a node in expanded format (fixed length
     * arcs).
     */
    boolean isExpandedTarget(Arc<T> follow, BytesReader in) throws IOException {
        if (targetHasArcs(follow) == false) {
            return false;
        } else {
            in.setPosition(follow.target());
            byte flags = in.readByte();
            return flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING;
        }
    }

    /** In-place read; returns the arc. */
    public Arc<T> readNextArc(Arc<T> arc, BytesReader in) throws IOException {
        if (arc.label() == END_LABEL) {
            // This was a fake inserted "final" arc
            if (arc.nextArc() <= 0) {
                throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
            }
            return readFirstRealTargetArc(arc.nextArc(), arc, in);
        } else {
            return readNextRealArc(arc, in);
        }
    }

    /** Peeks at next arc's label; does not alter arc. Do not call this if arc.isLast()! */
    int readNextArcLabel(Arc<T> arc, BytesReader in) throws IOException {
        assert arc.isLast() == false;

        if (arc.label() == END_LABEL) {
            // System.out.println(" nextArc fake " + arc.nextArc);
            // Next arc is the first arc of a node.
            // Position to read the first arc label.

            in.setPosition(arc.nextArc());
            byte flags = in.readByte();
            if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
                // System.out.println(" nextArc fixed length arc");
                // Special arc which is actually a node header for fixed length arcs.
                int numArcs = in.readVInt();
                if (version >= VERSION_VINT_TARGET) {
                    in.readVInt(); // Skip bytesPerArc.
                } else {
                    in.readInt(); // Skip bytesPerArc.
                }
                if (flags == ARCS_FOR_BINARY_SEARCH) {
                    in.readByte(); // Skip arc flags.
                } else {
                    in.skipBytes(getNumPresenceBytes(numArcs));
                }
            }
        } else {
            if (arc.bytesPerArc() != 0) {
                // System.out.println(" nextArc real array");
                // Arcs have fixed length.
                if (arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH) {
                    // Point to next arc, -1 to skip arc flags.
                    in.setPosition(arc.posArcsStart() - (1 + arc.arcIdx()) * arc.bytesPerArc() - 1);
                } else {
                    assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                    // Direct addressing node. The label is not stored but rather inferred
                    // based on first label and arc index in the range.
                    assert Arc.BitTable.assertIsValid(arc, in);
                    assert Arc.BitTable.isBitSet(arc.arcIdx(), arc, in);
                    int nextIndex = Arc.BitTable.nextBitSet(arc.arcIdx(), arc, in);
                    assert nextIndex != -1;
                    return arc.firstLabel() + nextIndex;
                }
            } else {
                // Arcs have variable length.
                // System.out.println(" nextArc real list");
                // Position to next arc, -1 to skip flags.
                in.setPosition(arc.nextArc() - 1);
            }
        }
        return readLabel(in);
    }

    public Arc<T> readArcByIndex(Arc<T> arc, final BytesReader in, int idx) throws IOException {
        assert arc.bytesPerArc() > 0;
        assert arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH;
        assert idx >= 0 && idx < arc.numArcs();
        in.setPosition(arc.posArcsStart() - idx * arc.bytesPerArc());
        arc.arcIdx = idx;
        arc.flags = in.readByte();
        return readArc(arc, in);
    }

    /**
     * Reads a present direct addressing node arc, with the provided index in the label range.
     *
     * @param rangeIndex The index of the arc in the label range. It must be present. The real arc
     *     offset is computed based on the presence bits of the direct addressing node.
     */
    public Arc<T> readArcByDirectAddressing(Arc<T> arc, final BytesReader in, int rangeIndex) throws IOException {
        assert Arc.BitTable.assertIsValid(arc, in);
        assert rangeIndex >= 0 && rangeIndex < arc.numArcs();
        assert Arc.BitTable.isBitSet(rangeIndex, arc, in);
        int presenceIndex = Arc.BitTable.countBitsUpTo(rangeIndex, arc, in);
        return readArcByDirectAddressing(arc, in, rangeIndex, presenceIndex);
    }

    /**
     * Reads a present direct addressing node arc, with the provided index in the label range and its
     * corresponding presence index (which is the count of presence bits before it).
     */
    private Arc<T> readArcByDirectAddressing(Arc<T> arc, final BytesReader in, int rangeIndex, int presenceIndex) throws IOException {
        in.setPosition(arc.posArcsStart() - presenceIndex * arc.bytesPerArc());
        arc.arcIdx = rangeIndex;
        arc.presenceIndex = presenceIndex;
        arc.flags = in.readByte();
        return readArc(arc, in);
    }

    /**
     * Reads the last arc of a direct addressing node. This method is equivalent to call {@link
     * #readArcByDirectAddressing(Arc, BytesReader, int)} with {@code rangeIndex} equal to {@code
     * arc.numArcs() - 1}, but it is faster.
     */
    public Arc<T> readLastArcByDirectAddressing(Arc<T> arc, final BytesReader in) throws IOException {
        assert Arc.BitTable.assertIsValid(arc, in);
        int presenceIndex = Arc.BitTable.countBits(arc, in) - 1;
        return readArcByDirectAddressing(arc, in, arc.numArcs() - 1, presenceIndex);
    }

    /** Never returns null, but you should never call this if arc.isLast() is true. */
    public Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {

        // TODO: can't assert this because we call from readFirstArc
        // assert !flag(arc.flags, BIT_LAST_ARC);

        switch (arc.nodeFlags()) {
            case ARCS_FOR_BINARY_SEARCH:
                assert arc.bytesPerArc() > 0;
                arc.arcIdx++;
                assert arc.arcIdx() >= 0 && arc.arcIdx() < arc.numArcs();
                in.setPosition(arc.posArcsStart() - arc.arcIdx() * arc.bytesPerArc());
                arc.flags = in.readByte();
                break;

            case ARCS_FOR_DIRECT_ADDRESSING:
                assert Arc.BitTable.assertIsValid(arc, in);
                assert arc.arcIdx() == -1 || Arc.BitTable.isBitSet(arc.arcIdx(), arc, in);
                int nextIndex = Arc.BitTable.nextBitSet(arc.arcIdx(), arc, in);
                return readArcByDirectAddressing(arc, in, nextIndex, arc.presenceIndex + 1);

            default:
                // Variable length arcs - linear search.
                assert arc.bytesPerArc() == 0;
                in.setPosition(arc.nextArc());
                arc.flags = in.readByte();
        }
        return readArc(arc, in);
    }

    /**
     * Reads an arc. <br>
     * Precondition: The arc flags byte has already been read and set; the given BytesReader is
     * positioned just after the arc flags byte.
     */
    private Arc<T> readArc(Arc<T> arc, BytesReader in) throws IOException {
        if (arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING) {
            arc.label = arc.firstLabel() + arc.arcIdx();
        } else {
            arc.label = readLabel(in);
        }

        if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
            arc.output = outputs.read(in);
        } else {
            arc.output = outputs.getNoOutput();
        }

        if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
            arc.nextFinalOutput = outputs.readFinalOutput(in);
        } else {
            arc.nextFinalOutput = outputs.getNoOutput();
        }

        if (arc.flag(BIT_STOP_NODE)) {
            if (arc.flag(BIT_FINAL_ARC)) {
                arc.target = FINAL_END_NODE;
            } else {
                arc.target = NON_FINAL_END_NODE;
            }
            arc.nextArc = in.getPosition(); // Only useful for list.
        } else if (arc.flag(BIT_TARGET_NEXT)) {
            arc.nextArc = in.getPosition(); // Only useful for list.
            // TODO: would be nice to make this lazy -- maybe
            // caller doesn't need the target and is scanning arcs...
            if (arc.flag(BIT_LAST_ARC) == false) {
                if (arc.bytesPerArc() == 0) {
                    // must scan
                    seekToNextNode(in);
                } else {
                    int numArcs = arc.nodeFlags == ARCS_FOR_DIRECT_ADDRESSING ? Arc.BitTable.countBits(arc, in) : arc.numArcs();
                    in.setPosition(arc.posArcsStart() - arc.bytesPerArc() * numArcs);
                }
            }
            arc.target = in.getPosition();
        } else {
            arc.target = readUnpackedNodeTarget(in);
            arc.nextArc = in.getPosition(); // Only useful for list.
        }
        return arc;
    }

    static <T> Arc<T> readEndArc(Arc<T> follow, Arc<T> arc) {
        if (follow.isFinal()) {
            if (follow.target() <= 0) {
                arc.flags = FST.BIT_LAST_ARC;
            } else {
                arc.flags = 0;
                // NOTE: nextArc is a node (not an address!) in this case:
                arc.nextArc = follow.target();
            }
            arc.output = follow.nextFinalOutput();
            arc.label = FST.END_LABEL;
            return arc;
        } else {
            return null;
        }
    }

    // TODO: could we somehow [partially] tableize arc lookups
    // like automaton?

    /**
     * Finds an arc leaving the incoming arc, replacing the arc in place. This returns null if the arc
     * was not found, else the incoming arc.
     */
    public Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {

        if (labelToMatch == END_LABEL) {
            if (follow.isFinal()) {
                if (follow.target() <= 0) {
                    arc.flags = BIT_LAST_ARC;
                } else {
                    arc.flags = 0;
                    // NOTE: nextArc is a node (not an address!) in this case:
                    arc.nextArc = follow.target();
                }
                arc.output = follow.nextFinalOutput();
                arc.label = END_LABEL;
                arc.nodeFlags = arc.flags;
                return arc;
            } else {
                return null;
            }
        }

        if (targetHasArcs(follow) == false) {
            return null;
        }

        in.setPosition(follow.target());

        // System.out.println("fta label=" + (char) labelToMatch);

        byte flags = arc.nodeFlags = in.readByte();
        if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
            arc.numArcs = in.readVInt(); // This is in fact the label range.
            if (version >= VERSION_VINT_TARGET) {
                arc.bytesPerArc = in.readVInt();
            } else {
                arc.bytesPerArc = in.readInt();
            }
            readPresenceBytes(arc, in);
            arc.firstLabel = readLabel(in);
            arc.posArcsStart = in.getPosition();

            int arcIndex = labelToMatch - arc.firstLabel();
            if (arcIndex < 0 || arcIndex >= arc.numArcs()) {
                return null; // Before or after label range.
            } else if (Arc.BitTable.isBitSet(arcIndex, arc, in) == false) {
                return null; // Arc missing in the range.
            }
            return readArcByDirectAddressing(arc, in, arcIndex);
        } else if (flags == ARCS_FOR_BINARY_SEARCH) {
            arc.numArcs = in.readVInt();
            if (version >= VERSION_VINT_TARGET) {
                arc.bytesPerArc = in.readVInt();
            } else {
                arc.bytesPerArc = in.readInt();
            }
            arc.posArcsStart = in.getPosition();

            // Array is sparse; do binary search:
            int low = 0;
            int high = arc.numArcs() - 1;
            while (low <= high) {
                // System.out.println(" cycle");
                int mid = (low + high) >>> 1;
                // +1 to skip over flags
                in.setPosition(arc.posArcsStart() - (arc.bytesPerArc() * mid + 1));
                int midLabel = readLabel(in);
                final int cmp = midLabel - labelToMatch;
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    arc.arcIdx = mid - 1;
                    // System.out.println(" found!");
                    return readNextRealArc(arc, in);
                }
            }
            return null;
        }

        // Linear scan
        readFirstRealTargetArc(follow.target(), arc, in);

        while (true) {
            // System.out.println(" non-bs cycle");
            // TODO: we should fix this code to not have to create
            // object for the output of every arc we scan... only
            // for the matching arc, if found
            if (arc.label() == labelToMatch) {
                // System.out.println(" found!");
                return arc;
            } else if (arc.label() > labelToMatch) {
                return null;
            } else if (arc.isLast()) {
                return null;
            } else {
                readNextRealArc(arc, in);
            }
        }
    }

    private void seekToNextNode(BytesReader in) throws IOException {

        while (true) {

            final int flags = in.readByte();
            readLabel(in);

            if (flag(flags, BIT_ARC_HAS_OUTPUT)) {
                outputs.skipOutput(in);
            }

            if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
                outputs.skipFinalOutput(in);
            }

            if (flag(flags, BIT_STOP_NODE) == false && flag(flags, BIT_TARGET_NEXT) == false) {
                readUnpackedNodeTarget(in);
            }

            if (flag(flags, BIT_LAST_ARC)) {
                return;
            }
        }
    }

    /** Returns a {@link BytesReader} for this FST, positioned at position 0. */
    public BytesReader getBytesReader() {
        if (this.fstStore != null) {
            return this.fstStore.getReverseBytesReader();
        } else {
            return bytes.getReverseReader();
        }
    }

    /** Reads bytes stored in an FST. */
    public abstract static class BytesReader extends DataInput {
        /** Get current read position. */
        public abstract long getPosition();

        /** Set current read position. */
        public abstract void setPosition(long pos);

        /** Returns true if this reader uses reversed bytes under-the-hood. */
        public abstract boolean reversed();
    }
}
