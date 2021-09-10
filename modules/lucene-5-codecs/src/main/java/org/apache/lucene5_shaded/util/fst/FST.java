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
package org.apache.lucene5_shaded.util.fst;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.store.ByteArrayDataOutput;
import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.store.InputStreamDataInput;
import org.apache.lucene5_shaded.store.OutputStreamDataOutput;
import org.apache.lucene5_shaded.store.RAMOutputStream;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.Constants;
import org.apache.lucene5_shaded.util.PriorityQueue;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.packed.GrowableWriter;
import org.apache.lucene5_shaded.util.packed.PackedInts;

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

/** Represents an finite state machine (FST), using a
 *  compact byte[] format.
 *  <p> The format is similar to what's used by Morfologik
 *  (http://sourceforge.net/projects/morfologik).
 *  
 *  <p> See the {@link org.apache.lucene5_shaded.util.fst package
 *      documentation} for some simple examples.
 *
 * @lucene.experimental
 */
public final class FST<T> implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FST.class);
  private static final long ARC_SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Arc.class);

  /** Specifies allowed range of each int input label for
   *  this FST. */
  public static enum INPUT_TYPE {BYTE1, BYTE2, BYTE4};

  static final int BIT_FINAL_ARC = 1 << 0;
  static final int BIT_LAST_ARC = 1 << 1;
  static final int BIT_TARGET_NEXT = 1 << 2;

  // TODO: we can free up a bit if we can nuke this:
  static final int BIT_STOP_NODE = 1 << 3;

  /** This flag is set if the arc has an output. */
  public static final int BIT_ARC_HAS_OUTPUT = 1 << 4;

  static final int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

  // Arcs are stored as fixed-size (per entry) array, so
  // that we can find an arc using binary search.  We do
  // this when number of arcs is > NUM_ARCS_ARRAY:

  // If set, the target node is delta coded vs current
  // position:
  private static final int BIT_TARGET_DELTA = 1 << 6;

  // We use this as a marker (because this one flag is
  // illegal by itself ...):
  private static final byte ARCS_AS_FIXED_ARRAY = BIT_ARC_HAS_FINAL_OUTPUT;

  /**
   * @see #shouldExpand(Builder, Builder.UnCompiledNode)
   */
  static final int FIXED_ARRAY_SHALLOW_DISTANCE = 3; // 0 => only root node.

  /**
   * @see #shouldExpand(Builder, Builder.UnCompiledNode)
   */
  static final int FIXED_ARRAY_NUM_ARCS_SHALLOW = 5;

  /**
   * @see #shouldExpand(Builder, Builder.UnCompiledNode)
   */
  static final int FIXED_ARRAY_NUM_ARCS_DEEP = 10;

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

  private static final int VERSION_CURRENT = VERSION_NO_NODE_ARC_COUNTS;

  // Never serialized; just used to represent the virtual
  // final node w/ no arcs:
  private static final long FINAL_END_NODE = -1;

  // Never serialized; just used to represent the virtual
  // non-final node w/ no arcs:
  private static final long NON_FINAL_END_NODE = 0;

  /** If arc has this label then that arc is final/accepted */
  public static final int END_LABEL = -1;

  public final INPUT_TYPE inputType;

  // if non-null, this FST accepts the empty string and
  // produces this output
  T emptyOutput;

  /** A {@link BytesStore}, used during building, or during reading when
   *  the FST is very large (more than 1 GB).  If the FST is less than 1
   *  GB then bytesArray is set instead. */
  final BytesStore bytes;

  /** Used at read time when the FST fits into a single byte[]. */
  final byte[] bytesArray;

  private long startNode = -1;

  public final Outputs<T> outputs;

  private final boolean packed;
  private PackedInts.Reader nodeRefToAddress;

  private Arc<T> cachedRootArcs[];

  /** Represents a single arc. */
  public static final class Arc<T> {
    public int label;
    public T output;

    // From node (ord or address); currently only used when
    // building an FST w/ willPackFST=true:
    long node;

    /** To node (ord or address) */
    public long target;

    byte flags;
    public T nextFinalOutput;

    // address (into the byte[]), or ord/address if label == END_LABEL
    long nextArc;

    /** Where the first arc in the array starts; only valid if
     *  bytesPerArc != 0 */
    public long posArcsStart;
    
    /** Non-zero if this arc is part of an array, which means all
     *  arcs for the node are encoded with a fixed number of bytes so
     *  that we can random access by index.  We do when there are enough
     *  arcs leaving one node.  It wastes some bytes but gives faster
     *  lookups. */
    public int bytesPerArc;

    /** Where we are in the array; only valid if bytesPerArc != 0. */
    public int arcIdx;

    /** How many arcs in the array; only valid if bytesPerArc != 0. */
    public int numArcs;

    /** Returns this */
    public Arc<T> copyFrom(Arc<T> other) {
      node = other.node;
      label = other.label;
      target = other.target;
      flags = other.flags;
      output = other.output;
      nextFinalOutput = other.nextFinalOutput;
      nextArc = other.nextArc;
      bytesPerArc = other.bytesPerArc;
      if (bytesPerArc != 0) {
        posArcsStart = other.posArcsStart;
        arcIdx = other.arcIdx;
        numArcs = other.numArcs;
      }
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
      b.append("node=" + node);
      b.append(" target=" + target);
      b.append(" label=0x" + Integer.toHexString(label));
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
        b.append(" output=" + output);
      }
      if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
        b.append(" nextFinalOutput=" + nextFinalOutput);
      }
      if (bytesPerArc != 0) {
        b.append(" arcArray(idx=" + arcIdx + " of " + numArcs + ")");
      }
      return b.toString();
    }
  };

  private static boolean flag(int flags, int bit) {
    return (flags & bit) != 0;
  }

  private GrowableWriter nodeAddress;

  // TODO: we could be smarter here, and prune periodically
  // as we go; high in-count nodes will "usually" become
  // clear early on:
  private GrowableWriter inCounts;

  private final int version;

  // make a new empty FST, for building; Builder invokes
  // this ctor
  FST(INPUT_TYPE inputType, Outputs<T> outputs, boolean willPackFST, float acceptableOverheadRatio, int bytesPageBits) {
    this.inputType = inputType;
    this.outputs = outputs;
    version = VERSION_CURRENT;
    bytesArray = null;
    bytes = new BytesStore(bytesPageBits);
    // pad: ensure no node gets address 0 which is reserved to mean
    // the stop state w/ no arcs
    bytes.writeByte((byte) 0);
    if (willPackFST) {
      nodeAddress = new GrowableWriter(15, 8, acceptableOverheadRatio);
      inCounts = new GrowableWriter(1, 8, acceptableOverheadRatio);
    } else {
      nodeAddress = null;
      inCounts = null;
    }

    emptyOutput = null;
    packed = false;
    nodeRefToAddress = null;
  }

  public static final int DEFAULT_MAX_BLOCK_BITS = Constants.JRE_IS_64BIT ? 30 : 28;

  /** Load a previously saved FST. */
  public FST(DataInput in, Outputs<T> outputs) throws IOException {
    this(in, outputs, DEFAULT_MAX_BLOCK_BITS);
  }

  /** Load a previously saved FST; maxBlockBits allows you to
   *  control the size of the byte[] pages used to hold the FST bytes. */
  public FST(DataInput in, Outputs<T> outputs, int maxBlockBits) throws IOException {
    this.outputs = outputs;

    if (maxBlockBits < 1 || maxBlockBits > 30) {
      throw new IllegalArgumentException("maxBlockBits should be 1 .. 30; got " + maxBlockBits);
    }

    // NOTE: only reads most recent format; we don't have
    // back-compat promise for FSTs (they are experimental):
    version = CodecUtil.checkHeader(in, FILE_FORMAT_NAME, VERSION_PACKED, VERSION_NO_NODE_ARC_COUNTS);
    packed = in.readByte() == 1;
    if (in.readByte() == 1) {
      // accepts empty string
      // 1 KB blocks:
      BytesStore emptyBytes = new BytesStore(10);
      int numBytes = in.readVInt();
      emptyBytes.copyBytes(in, numBytes);

      // De-serialize empty-string output:
      BytesReader reader;
      if (packed) {
        reader = emptyBytes.getForwardReader();
      } else {
        reader = emptyBytes.getReverseReader();
        // NoOutputs uses 0 bytes when writing its output,
        // so we have to check here else BytesStore gets
        // angry:
        if (numBytes > 0) {
          reader.setPosition(numBytes-1);
        }
      }
      emptyOutput = outputs.readFinalOutput(reader);
    } else {
      emptyOutput = null;
    }
    final byte t = in.readByte();
    switch(t) {
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
      throw new IllegalStateException("invalid input type " + t);
    }
    if (packed) {
      nodeRefToAddress = PackedInts.getReader(in);
    } else {
      nodeRefToAddress = null;
    }
    startNode = in.readVLong();
    if (version < VERSION_NO_NODE_ARC_COUNTS) {
      in.readVLong();
      in.readVLong();
      in.readVLong();
    }

    long numBytes = in.readVLong();
    if (numBytes > 1 << maxBlockBits) {
      // FST is big: we need multiple pages
      bytes = new BytesStore(in, numBytes, 1<<maxBlockBits);
      bytesArray = null;
    } else {
      // FST fits into a single block: use ByteArrayBytesStoreReader for less overhead
      bytes = null;
      bytesArray = new byte[(int) numBytes];
      in.readBytes(bytesArray, 0, bytesArray.length);
    }
    
    cacheRootArcs();
  }

  public INPUT_TYPE getInputType() {
    return inputType;
  }

  private long ramBytesUsed(Arc<T>[] arcs) {
    long size = 0;
    if (arcs != null) {
      size += RamUsageEstimator.shallowSizeOf(arcs);
      for (Arc<T> arc : arcs) {
        if (arc != null) {
          size += ARC_SHALLOW_RAM_BYTES_USED;
          if (arc.output != null && arc.output != outputs.getNoOutput()) {
            size += outputs.ramBytesUsed(arc.output);
          }
          if (arc.nextFinalOutput != null && arc.nextFinalOutput != outputs.getNoOutput()) {
            size += outputs.ramBytesUsed(arc.nextFinalOutput);
          }
        }
      }
    }
    return size;
  }

  private int cachedArcsBytesUsed;

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES_USED;
    if (bytesArray != null) {
      size += bytesArray.length;
    } else {
      size += bytes.ramBytesUsed();
    }
    if (packed) {
      size += nodeRefToAddress.ramBytesUsed();
    } else if (nodeAddress != null) {
      size += nodeAddress.ramBytesUsed();
      size += inCounts.ramBytesUsed();
    }
    size += cachedArcsBytesUsed;
    return size;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    if (packed) {
      resources.add(Accountables.namedAccountable("node ref to address", nodeRefToAddress));
    } else if (nodeAddress != null) {
      resources.add(Accountables.namedAccountable("node addresses", nodeAddress));
      resources.add(Accountables.namedAccountable("in counts", inCounts));
    }
    return resources;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(input=" + inputType + ",output=" + outputs + ",packed=" + packed;
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
    cacheRootArcs();
  }

  private long getNodeAddress(long node) {
    if (nodeAddress != null) {
      // Deref
      return nodeAddress.get((int) node);
    } else {
      // Straight
      return node;
    }
  }
  
  // Optionally caches first 128 labels
  @SuppressWarnings({"rawtypes","unchecked"})
  private void cacheRootArcs() throws IOException {
    // We should only be called once per FST:
    assert cachedArcsBytesUsed == 0;

    final Arc<T> arc = new Arc<>();
    getFirstArc(arc);
    if (targetHasArcs(arc)) {
      final BytesReader in = getBytesReader();
      Arc<T>[] arcs = (Arc<T>[]) new Arc[0x80];
      readFirstRealTargetArc(arc.target, arc, in);
      int count = 0;
      while(true) {
        assert arc.label != END_LABEL;
        if (arc.label < arcs.length) {
          arcs[arc.label] = new Arc<T>().copyFrom(arc);
        } else {
          break;
        }
        if (arc.isLast()) {
          break;
        }
        readNextRealArc(arc, in);
        count++;
      }

      int cacheRAM = (int) ramBytesUsed(arcs);

      // Don't cache if there are only a few arcs or if the cache would use > 20% RAM of the FST itself:
      if (count >= FIXED_ARRAY_NUM_ARCS_SHALLOW && cacheRAM < ramBytesUsed()/5) {
        cachedRootArcs = arcs;
        cachedArcsBytesUsed = cacheRAM;
      }
    }
  }
  
  public T getEmptyOutput() {
    return emptyOutput;
  }

  void setEmptyOutput(T v) throws IOException {
    if (emptyOutput != null) {
      emptyOutput = outputs.merge(emptyOutput, v);
    } else {
      emptyOutput = v;
    }
  }

  public void save(DataOutput out) throws IOException {
    if (startNode == -1) {
      throw new IllegalStateException("call finish first");
    }
    if (nodeAddress != null) {
      throw new IllegalStateException("cannot save an FST pre-packed FST; it must first be packed");
    }
    if (packed && !(nodeRefToAddress instanceof PackedInts.Mutable)) {
      throw new IllegalStateException("cannot save a FST which has been loaded from disk ");
    }
    CodecUtil.writeHeader(out, FILE_FORMAT_NAME, VERSION_CURRENT);
    if (packed) {
      out.writeByte((byte) 1);
    } else {
      out.writeByte((byte) 0);
    }
    // TODO: really we should encode this as an arc, arriving
    // to the root node, instead of special casing here:
    if (emptyOutput != null) {
      // Accepts empty string
      out.writeByte((byte) 1);

      // Serialize empty-string output:
      RAMOutputStream ros = new RAMOutputStream();
      outputs.writeFinalOutput(emptyOutput, ros);
      
      byte[] emptyOutputBytes = new byte[(int) ros.getFilePointer()];
      ros.writeTo(emptyOutputBytes, 0);

      if (!packed) {
        // reverse
        final int stopAt = emptyOutputBytes.length/2;
        int upto = 0;
        while(upto < stopAt) {
          final byte b = emptyOutputBytes[upto];
          emptyOutputBytes[upto] = emptyOutputBytes[emptyOutputBytes.length-upto-1];
          emptyOutputBytes[emptyOutputBytes.length-upto-1] = b;
          upto++;
        }
      }
      out.writeVInt(emptyOutputBytes.length);
      out.writeBytes(emptyOutputBytes, 0, emptyOutputBytes.length);
    } else {
      out.writeByte((byte) 0);
    }
    final byte t;
    if (inputType == INPUT_TYPE.BYTE1) {
      t = 0;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      t = 1;
    } else {
      t = 2;
    }
    out.writeByte(t);
    if (packed) {
      ((PackedInts.Mutable) nodeRefToAddress).save(out);
    }
    out.writeVLong(startNode);
    if (bytes != null) {
      long numBytes = bytes.getPosition();
      out.writeVLong(numBytes);
      bytes.writeTo(out);
    } else {
      assert bytesArray != null;
      out.writeVLong(bytesArray.length);
      out.writeBytes(bytesArray, 0, bytesArray.length);
    }
  }
  
  /**
   * Writes an automaton to a file. 
   */
  public void save(final Path path) throws IOException {
    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
      save(new OutputStreamDataOutput(os));
    }
  }

  /**
   * Reads an automaton from a file. 
   */
  public static <T> FST<T> read(Path path, Outputs<T> outputs) throws IOException {
    try (InputStream is = Files.newInputStream(path)) {
      return new FST<>(new InputStreamDataInput(new BufferedInputStream(is)), outputs);
    }
  }

  private void writeLabel(DataOutput out, int v) throws IOException {
    assert v >= 0: "v=" + v;
    if (inputType == INPUT_TYPE.BYTE1) {
      assert v <= 255: "v=" + v;
      out.writeByte((byte) v);
    } else if (inputType == INPUT_TYPE.BYTE2) {
      assert v <= 65535: "v=" + v;
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
      v = in.readByte()&0xFF;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      // Unsigned short:
      v = in.readShort()&0xFFFF;
    } else { 
      v = in.readVInt();
    }
    return v;
  }

  /** returns true if the node at this address has any
   *  outgoing arcs */
  public static<T> boolean targetHasArcs(Arc<T> arc) {
    return arc.target > 0;
  }

  // serializes new node by appending its bytes to the end
  // of the current byte[]
  long addNode(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn) throws IOException {
    T NO_OUTPUT = outputs.getNoOutput();

    //System.out.println("FST.addNode pos=" + bytes.getPosition() + " numArcs=" + nodeIn.numArcs);
    if (nodeIn.numArcs == 0) {
      if (nodeIn.isFinal) {
        return FINAL_END_NODE;
      } else {
        return NON_FINAL_END_NODE;
      }
    }

    final long startAddress = builder.bytes.getPosition();
    //System.out.println("  startAddr=" + startAddress);

    final boolean doFixedArray = shouldExpand(builder, nodeIn);
    if (doFixedArray) {
      //System.out.println("  fixedArray");
      if (builder.reusedBytesPerArc.length < nodeIn.numArcs) {
        builder.reusedBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, 1)];
      }
    }

    builder.arcCount += nodeIn.numArcs;
    
    final int lastArc = nodeIn.numArcs-1;

    long lastArcStart = builder.bytes.getPosition();
    int maxBytesPerArc = 0;
    for(int arcIdx=0;arcIdx<nodeIn.numArcs;arcIdx++) {
      final Builder.Arc<T> arc = nodeIn.arcs[arcIdx];
      final Builder.CompiledNode target = (Builder.CompiledNode) arc.target;
      int flags = 0;
      //System.out.println("  arc " + arcIdx + " label=" + arc.label + " -> target=" + target.node);

      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;
      }

      if (builder.lastFrozenNode == target.node && !doFixedArray) {
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

      if (!targetHasArcs) {
        flags += BIT_STOP_NODE;
      } else if (inCounts != null) {
        inCounts.set((int) target.node, inCounts.get((int) target.node) + 1);
      }

      if (arc.output != NO_OUTPUT) {
        flags += BIT_ARC_HAS_OUTPUT;
      }

      builder.bytes.writeByte((byte) flags);
      writeLabel(builder.bytes, arc.label);

      // System.out.println("  write arc: label=" + (char) arc.label + " flags=" + flags + " target=" + target.node + " pos=" + bytes.getPosition() + " output=" + outputs.outputToString(arc.output));

      if (arc.output != NO_OUTPUT) {
        outputs.write(arc.output, builder.bytes);
        //System.out.println("    write output");
      }

      if (arc.nextFinalOutput != NO_OUTPUT) {
        //System.out.println("    write final output");
        outputs.writeFinalOutput(arc.nextFinalOutput, builder.bytes);
      }

      if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
        assert target.node > 0;
        //System.out.println("    write target");
        builder.bytes.writeVLong(target.node);
      }

      // just write the arcs "like normal" on first pass,
      // but record how many bytes each one took, and max
      // byte size:
      if (doFixedArray) {
        builder.reusedBytesPerArc[arcIdx] = (int) (builder.bytes.getPosition() - lastArcStart);
        lastArcStart = builder.bytes.getPosition();
        maxBytesPerArc = Math.max(maxBytesPerArc, builder.reusedBytesPerArc[arcIdx]);
        //System.out.println("    bytes=" + builder.reusedBytesPerArc[arcIdx]);
      }
    }
    
    // TODO: try to avoid wasteful cases: disable doFixedArray in that case
    /* 
     * 
     * LUCENE-4682: what is a fair heuristic here?
     * It could involve some of these:
     * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
     * 2. how much binSearch saves over scan: nodeIn.numArcs
     * 3. waste: numBytes vs numBytesExpanded
     * 
     * the one below just looks at #3
    if (doFixedArray) {
      // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
      int numBytes = lastArcStart - startAddress;
      int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
      if (numBytesExpanded > numBytes*1.25) {
        doFixedArray = false;
      }
    }
    */

    if (doFixedArray) {
      final int MAX_HEADER_SIZE = 11; // header(byte) + numArcs(vint) + numBytes(vint)
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed
      // byte size

      //System.out.println("write int @pos=" + (fixedArrayStart-4) + " numArcs=" + nodeIn.numArcs);
      // create the header
      // TODO: clean this up: or just rewind+reuse and deal with it
      byte header[] = new byte[MAX_HEADER_SIZE]; 
      ByteArrayDataOutput bad = new ByteArrayDataOutput(header);
      // write a "false" first arc:
      bad.writeByte(ARCS_AS_FIXED_ARRAY);
      bad.writeVInt(nodeIn.numArcs);
      bad.writeVInt(maxBytesPerArc);
      int headerLen = bad.getPosition();
      
      final long fixedArrayStart = startAddress + headerLen;

      // expand the arcs in place, backwards
      long srcPos = builder.bytes.getPosition();
      long destPos = fixedArrayStart + nodeIn.numArcs*maxBytesPerArc;
      assert destPos >= srcPos;
      if (destPos > srcPos) {
        builder.bytes.skipBytes((int) (destPos - srcPos));
        for(int arcIdx=nodeIn.numArcs-1;arcIdx>=0;arcIdx--) {
          destPos -= maxBytesPerArc;
          srcPos -= builder.reusedBytesPerArc[arcIdx];
          //System.out.println("  repack arcIdx=" + arcIdx + " srcPos=" + srcPos + " destPos=" + destPos);
          if (srcPos != destPos) {
            //System.out.println("  copy len=" + builder.reusedBytesPerArc[arcIdx]);
            assert destPos > srcPos: "destPos=" + destPos + " srcPos=" + srcPos + " arcIdx=" + arcIdx + " maxBytesPerArc=" + maxBytesPerArc + " reusedBytesPerArc[arcIdx]=" + builder.reusedBytesPerArc[arcIdx] + " nodeIn.numArcs=" + nodeIn.numArcs;
            builder.bytes.copyBytes(srcPos, destPos, builder.reusedBytesPerArc[arcIdx]);
          }
        }
      }
      
      // now write the header
      builder.bytes.writeBytes(startAddress, header, 0, headerLen);
    }

    final long thisNodeAddress = builder.bytes.getPosition()-1;

    builder.bytes.reverse(startAddress, thisNodeAddress);

    // PackedInts uses int as the index, so we cannot handle
    // > 2.1B nodes when packing:
    if (nodeAddress != null && builder.nodeCount == Integer.MAX_VALUE) {
      throw new IllegalStateException("cannot create a packed FST with more than 2.1 billion nodes");
    }

    builder.nodeCount++;
    final long node;
    if (nodeAddress != null) {

      // Nodes are addressed by 1+ord:
      if ((int) builder.nodeCount == nodeAddress.size()) {
        nodeAddress = nodeAddress.resize(ArrayUtil.oversize(nodeAddress.size() + 1, nodeAddress.getBitsPerValue()));
        inCounts = inCounts.resize(ArrayUtil.oversize(inCounts.size() + 1, inCounts.getBitsPerValue()));
      }
      nodeAddress.set((int) builder.nodeCount, thisNodeAddress);
      // System.out.println("  write nodeAddress[" + nodeCount + "] = " + endAddress);
      node = builder.nodeCount;
    } else {
      node = thisNodeAddress;
    }

    //System.out.println("  ret node=" + node + " address=" + thisNodeAddress + " nodeAddress=" + nodeAddress);
    return node;
  }

  /** Fills virtual 'start' arc, ie, an empty incoming arc to
   *  the FST's start node */
  public Arc<T> getFirstArc(Arc<T> arc) {
    T NO_OUTPUT = outputs.getNoOutput();

    if (emptyOutput != null) {
      arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
      arc.nextFinalOutput = emptyOutput;
      if (emptyOutput != NO_OUTPUT) {
        arc.flags |= BIT_ARC_HAS_FINAL_OUTPUT;
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

  /** Follows the <code>follow</code> arc and reads the last
   *  arc of its target; this changes the provided
   *  <code>arc</code> (2nd arg) in-place and returns it.
   * 
   * @return Returns the second argument
   * (<code>arc</code>). */
  public Arc<T> readLastTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    //System.out.println("readLast");
    if (!targetHasArcs(follow)) {
      //System.out.println("  end node");
      assert follow.isFinal();
      arc.label = END_LABEL;
      arc.target = FINAL_END_NODE;
      arc.output = follow.nextFinalOutput;
      arc.flags = BIT_LAST_ARC;
      return arc;
    } else {
      in.setPosition(getNodeAddress(follow.target));
      arc.node = follow.target;
      final byte b = in.readByte();
      if (b == ARCS_AS_FIXED_ARRAY) {
        // array: jump straight to end
        arc.numArcs = in.readVInt();
        if (packed || version >= VERSION_VINT_TARGET) {
          arc.bytesPerArc = in.readVInt();
        } else {
          arc.bytesPerArc = in.readInt();
        }
        //System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
        arc.posArcsStart = in.getPosition();
        arc.arcIdx = arc.numArcs - 2;
      } else {
        arc.flags = b;
        // non-array: linear scan
        arc.bytesPerArc = 0;
        //System.out.println("  scan");
        while(!arc.isLast()) {
          // skip this arc:
          readLabel(in);
          if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
            outputs.skipOutput(in);
          }
          if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
            outputs.skipFinalOutput(in);
          }
          if (arc.flag(BIT_STOP_NODE)) {
          } else if (arc.flag(BIT_TARGET_NEXT)) {
          } else if (packed) {
            in.readVLong();
          } else {
            readUnpackedNodeTarget(in);
          }
          arc.flags = in.readByte();
        }
        // Undo the byte flags we read:
        in.skipBytes(-1);
        arc.nextArc = in.getPosition();
      }
      readNextRealArc(arc, in);
      assert arc.isLast();
      return arc;
    }
  }

  private long readUnpackedNodeTarget(BytesReader in) throws IOException {
    long target;
    if (version < VERSION_VINT_TARGET) {
      target = in.readInt();
    } else {
      target = in.readVLong();
    }
    return target;
  }

  /**
   * Follow the <code>follow</code> arc and read the first arc of its target;
   * this changes the provided <code>arc</code> (2nd arg) in-place and returns
   * it.
   * 
   * @return Returns the second argument (<code>arc</code>).
   */
  public Arc<T> readFirstTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    //int pos = address;
    //System.out.println("    readFirstTarget follow.target=" + follow.target + " isFinal=" + follow.isFinal());
    if (follow.isFinal()) {
      // Insert "fake" final first arc:
      arc.label = END_LABEL;
      arc.output = follow.nextFinalOutput;
      arc.flags = BIT_FINAL_ARC;
      if (follow.target <= 0) {
        arc.flags |= BIT_LAST_ARC;
      } else {
        arc.node = follow.target;
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target;
      }
      arc.target = FINAL_END_NODE;
      //System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" + arc.isLast() + " output=" + outputs.outputToString(arc.output));
      return arc;
    } else {
      return readFirstRealTargetArc(follow.target, arc, in);
    }
  }

  public Arc<T> readFirstRealTargetArc(long node, Arc<T> arc, final BytesReader in) throws IOException {
    final long address = getNodeAddress(node);
    in.setPosition(address);
    //System.out.println("  readFirstRealTargtArc address="
    //+ address);
    //System.out.println("   flags=" + arc.flags);
    arc.node = node;

    if (in.readByte() == ARCS_AS_FIXED_ARRAY) {
      //System.out.println("  fixedArray");
      // this is first arc in a fixed-array
      arc.numArcs = in.readVInt();
      if (packed || version >= VERSION_VINT_TARGET) {
        arc.bytesPerArc = in.readVInt();
      } else {
        arc.bytesPerArc = in.readInt();
      }
      arc.arcIdx = -1;
      arc.nextArc = arc.posArcsStart = in.getPosition();
      //System.out.println("  bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + " arcsStart=" + pos);
    } else {
      //arc.flags = b;
      arc.nextArc = address;
      arc.bytesPerArc = 0;
    }

    return readNextRealArc(arc, in);
  }

  /**
   * Checks if <code>arc</code>'s target state is in expanded (or vector) format. 
   * 
   * @return Returns <code>true</code> if <code>arc</code> points to a state in an
   * expanded array format.
   */
  boolean isExpandedTarget(Arc<T> follow, BytesReader in) throws IOException {
    if (!targetHasArcs(follow)) {
      return false;
    } else {
      in.setPosition(getNodeAddress(follow.target));
      return in.readByte() == ARCS_AS_FIXED_ARRAY;
    }
  }

  /** In-place read; returns the arc. */
  public Arc<T> readNextArc(Arc<T> arc, BytesReader in) throws IOException {
    if (arc.label == END_LABEL) {
      // This was a fake inserted "final" arc
      if (arc.nextArc <= 0) {
        throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
      }
      return readFirstRealTargetArc(arc.nextArc, arc, in);
    } else {
      return readNextRealArc(arc, in);
    }
  }

  /** Peeks at next arc's label; does not alter arc.  Do
   *  not call this if arc.isLast()! */
  public int readNextArcLabel(Arc<T> arc, BytesReader in) throws IOException {
    assert !arc.isLast();

    if (arc.label == END_LABEL) {
      //System.out.println("    nextArc fake " +
      //arc.nextArc);
      
      long pos = getNodeAddress(arc.nextArc);
      in.setPosition(pos);

      final byte b = in.readByte();
      if (b == ARCS_AS_FIXED_ARRAY) {
        //System.out.println("    nextArc fixed array");
        in.readVInt();

        // Skip bytesPerArc:
        if (packed || version >= VERSION_VINT_TARGET) {
          in.readVInt();
        } else {
          in.readInt();
        }
      } else {
        in.setPosition(pos);
      }
    } else {
      if (arc.bytesPerArc != 0) {
        //System.out.println("    nextArc real array");
        // arcs are at fixed entries
        in.setPosition(arc.posArcsStart);
        in.skipBytes((1+arc.arcIdx)*arc.bytesPerArc);
      } else {
        // arcs are packed
        //System.out.println("    nextArc real packed");
        in.setPosition(arc.nextArc);
      }
    }
    // skip flags
    in.readByte();
    return readLabel(in);
  }

  /** Never returns null, but you should never call this if
   *  arc.isLast() is true. */
  public Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {

    // TODO: can't assert this because we call from readFirstArc
    // assert !flag(arc.flags, BIT_LAST_ARC);

    // this is a continuing arc in a fixed array
    if (arc.bytesPerArc != 0) {
      // arcs are at fixed entries
      arc.arcIdx++;
      assert arc.arcIdx < arc.numArcs;
      in.setPosition(arc.posArcsStart);
      in.skipBytes(arc.arcIdx*arc.bytesPerArc);
    } else {
      // arcs are packed
      in.setPosition(arc.nextArc);
    }
    arc.flags = in.readByte();
    arc.label = readLabel(in);

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
      arc.nextArc = in.getPosition();
    } else if (arc.flag(BIT_TARGET_NEXT)) {
      arc.nextArc = in.getPosition();
      // TODO: would be nice to make this lazy -- maybe
      // caller doesn't need the target and is scanning arcs...
      if (nodeAddress == null) {
        if (!arc.flag(BIT_LAST_ARC)) {
          if (arc.bytesPerArc == 0) {
            // must scan
            seekToNextNode(in);
          } else {
            in.setPosition(arc.posArcsStart);
            in.skipBytes(arc.bytesPerArc * arc.numArcs);
          }
        }
        arc.target = in.getPosition();
      } else {
        arc.target = arc.node - 1;
        assert arc.target > 0;
      }
    } else {
      if (packed) {
        final long pos = in.getPosition();
        final long code = in.readVLong();
        if (arc.flag(BIT_TARGET_DELTA)) {
          // Address is delta-coded from current address:
          arc.target = pos + code;
          //System.out.println("    delta pos=" + pos + " delta=" + code + " target=" + arc.target);
        } else if (code < nodeRefToAddress.size()) {
          // Deref
          arc.target = nodeRefToAddress.get((int) code);
          //System.out.println("    deref code=" + code + " target=" + arc.target);
        } else {
          // Absolute
          arc.target = code;
          //System.out.println("    abs code=" + code);
        }
      } else {
        arc.target = readUnpackedNodeTarget(in);
      }
      arc.nextArc = in.getPosition();
    }
    return arc;
  }

  // LUCENE-5152: called only from asserts, to validate that the
  // non-cached arc lookup would produce the same result, to
  // catch callers that illegally modify shared structures with
  // the result (we shallow-clone the Arc itself, but e.g. a BytesRef
  // output is still shared):
  private boolean assertRootCachedArc(int label, Arc<T> cachedArc) throws IOException {
    Arc<T> arc = new Arc<>();
    getFirstArc(arc);
    BytesReader in = getBytesReader();
    Arc<T> result = findTargetArc(label, arc, arc, in, false);
    if (result == null) {
      assert cachedArc == null;
    } else {
      assert cachedArc != null;
      assert cachedArc.arcIdx == result.arcIdx;
      assert cachedArc.bytesPerArc == result.bytesPerArc;
      assert cachedArc.flags == result.flags;
      assert cachedArc.label == result.label;
      assert cachedArc.nextArc == result.nextArc;
      assert cachedArc.nextFinalOutput.equals(result.nextFinalOutput);
      assert cachedArc.node == result.node;
      assert cachedArc.numArcs == result.numArcs;
      assert cachedArc.output.equals(result.output);
      assert cachedArc.posArcsStart == result.posArcsStart;
      assert cachedArc.target == result.target;
    }

    return true;
  }

  // TODO: could we somehow [partially] tableize arc lookups
  // like automaton?

  /** Finds an arc leaving the incoming arc, replacing the arc in place.
   *  This returns null if the arc was not found, else the incoming arc. */
  public Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    return findTargetArc(labelToMatch, follow, arc, in, true);
  }

  /** Finds an arc leaving the incoming arc, replacing the arc in place.
   *  This returns null if the arc was not found, else the incoming arc. */
  private Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in, boolean useRootArcCache) throws IOException {

    if (labelToMatch == END_LABEL) {
      if (follow.isFinal()) {
        if (follow.target <= 0) {
          arc.flags = BIT_LAST_ARC;
        } else {
          arc.flags = 0;
          // NOTE: nextArc is a node (not an address!) in this case:
          arc.nextArc = follow.target;
          arc.node = follow.target;
        }
        arc.output = follow.nextFinalOutput;
        arc.label = END_LABEL;
        return arc;
      } else {
        return null;
      }
    }

    // Short-circuit if this arc is in the root arc cache:
    if (useRootArcCache && cachedRootArcs != null && follow.target == startNode && labelToMatch < cachedRootArcs.length) {
      final Arc<T> result = cachedRootArcs[labelToMatch];

      // LUCENE-5152: detect tricky cases where caller
      // modified previously returned cached root-arcs:
      assert assertRootCachedArc(labelToMatch, result);

      if (result == null) {
        return null;
      } else {
        arc.copyFrom(result);
        return arc;
      }
    }

    if (!targetHasArcs(follow)) {
      return null;
    }

    in.setPosition(getNodeAddress(follow.target));

    arc.node = follow.target;

    // System.out.println("fta label=" + (char) labelToMatch);

    if (in.readByte() == ARCS_AS_FIXED_ARRAY) {
      // Arcs are full array; do binary search:
      arc.numArcs = in.readVInt();
      if (packed || version >= VERSION_VINT_TARGET) {
        arc.bytesPerArc = in.readVInt();
      } else {
        arc.bytesPerArc = in.readInt();
      }
      arc.posArcsStart = in.getPosition();
      int low = 0;
      int high = arc.numArcs-1;
      while (low <= high) {
        //System.out.println("    cycle");
        int mid = (low + high) >>> 1;
        in.setPosition(arc.posArcsStart);
        in.skipBytes(arc.bytesPerArc*mid + 1);
        int midLabel = readLabel(in);
        final int cmp = midLabel - labelToMatch;
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          arc.arcIdx = mid-1;
          //System.out.println("    found!");
          return readNextRealArc(arc, in);
        }
      }

      return null;
    }

    // Linear scan
    readFirstRealTargetArc(follow.target, arc, in);

    while(true) {
      //System.out.println("  non-bs cycle");
      // TODO: we should fix this code to not have to create
      // object for the output of every arc we scan... only
      // for the matching arc, if found
      if (arc.label == labelToMatch) {
        //System.out.println("    found!");
        return arc;
      } else if (arc.label > labelToMatch) {
        return null;
      } else if (arc.isLast()) {
        return null;
      } else {
        readNextRealArc(arc, in);
      }
    }
  }

  private void seekToNextNode(BytesReader in) throws IOException {

    while(true) {

      final int flags = in.readByte();
      readLabel(in);

      if (flag(flags, BIT_ARC_HAS_OUTPUT)) {
        outputs.skipOutput(in);
      }

      if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
        outputs.skipFinalOutput(in);
      }

      if (!flag(flags, BIT_STOP_NODE) && !flag(flags, BIT_TARGET_NEXT)) {
        if (packed) {
          in.readVLong();
        } else {
          readUnpackedNodeTarget(in);
        }
      }

      if (flag(flags, BIT_LAST_ARC)) {
        return;
      }
    }
  }

  /**
   * Nodes will be expanded if their depth (distance from the root node) is
   * &lt;= this value and their number of arcs is &gt;=
   * {@link #FIXED_ARRAY_NUM_ARCS_SHALLOW}.
   * 
   * <p>
   * Fixed array consumes more RAM but enables binary search on the arcs
   * (instead of a linear scan) on lookup by arc label.
   * 
   * @return <code>true</code> if <code>node</code> should be stored in an
   *         expanded (array) form.
   * 
   * @see #FIXED_ARRAY_NUM_ARCS_DEEP
   * @see Builder.UnCompiledNode#depth
   */
  private boolean shouldExpand(Builder<T> builder, Builder.UnCompiledNode<T> node) {
    return builder.allowArrayArcs &&
      ((node.depth <= FIXED_ARRAY_SHALLOW_DISTANCE && node.numArcs >= FIXED_ARRAY_NUM_ARCS_SHALLOW) || 
       node.numArcs >= FIXED_ARRAY_NUM_ARCS_DEEP);
  }
  
  /** Returns a {@link BytesReader} for this FST, positioned at
   *  position 0. */
  public BytesReader getBytesReader() {
    if (packed) {
      if (bytesArray != null) {
        return new ForwardBytesReader(bytesArray);
      } else {
        return bytes.getForwardReader();
      }
    } else {
      if (bytesArray != null) {
        return new ReverseBytesReader(bytesArray);
      } else {
        return bytes.getReverseReader();
      }
    }
  }

  /** Reads bytes stored in an FST. */
  public static abstract class BytesReader extends DataInput {
    /** Get current read position. */
    public abstract long getPosition();

    /** Set current read position. */
    public abstract void setPosition(long pos);

    /** Returns true if this reader uses reversed bytes
     *  under-the-hood. */
    public abstract boolean reversed();
  }

  /*
  public void countSingleChains() throws IOException {
    // TODO: must assert this FST was built with
    // "willRewrite"

    final List<ArcAndState<T>> queue = new ArrayList<>();

    // TODO: use bitset to not revisit nodes already
    // visited

    FixedBitSet seen = new FixedBitSet(1+nodeCount);
    int saved = 0;

    queue.add(new ArcAndState<T>(getFirstArc(new Arc<T>()), new IntsRef()));
    Arc<T> scratchArc = new Arc<>();
    while(queue.size() > 0) {
      //System.out.println("cycle size=" + queue.size());
      //for(ArcAndState<T> ent : queue) {
      //  System.out.println("  " + Util.toBytesRef(ent.chain, new BytesRef()));
      //  }
      final ArcAndState<T> arcAndState = queue.get(queue.size()-1);
      seen.set(arcAndState.arc.node);
      final BytesRef br = Util.toBytesRef(arcAndState.chain, new BytesRef());
      if (br.length > 0 && br.bytes[br.length-1] == -1) {
        br.length--;
      }
      //System.out.println("  top node=" + arcAndState.arc.target + " chain=" + br.utf8ToString());
      if (targetHasArcs(arcAndState.arc) && !seen.get(arcAndState.arc.target)) {
        // push
        readFirstTargetArc(arcAndState.arc, scratchArc);
        //System.out.println("  push label=" + (char) scratchArc.label);
        //System.out.println("    tonode=" + scratchArc.target + " last?=" + scratchArc.isLast());
        
        final IntsRef chain = IntsRef.deepCopyOf(arcAndState.chain);
        chain.grow(1+chain.length);
        // TODO
        //assert scratchArc.label != END_LABEL;
        chain.ints[chain.length] = scratchArc.label;
        chain.length++;

        if (scratchArc.isLast()) {
          if (scratchArc.target != -1 && inCounts[scratchArc.target] == 1) {
            //System.out.println("    append");
          } else {
            if (arcAndState.chain.length > 1) {
              saved += chain.length-2;
              try {
                System.out.println("chain: " + Util.toBytesRef(chain, new BytesRef()).utf8ToString());
              } catch (AssertionError ae) {
                System.out.println("chain: " + Util.toBytesRef(chain, new BytesRef()));
              }
            }
            chain.length = 0;
          }
        } else {
          //System.out.println("    reset");
          if (arcAndState.chain.length > 1) {
            saved += arcAndState.chain.length-2;
            try {
              System.out.println("chain: " + Util.toBytesRef(arcAndState.chain, new BytesRef()).utf8ToString());
            } catch (AssertionError ae) {
              System.out.println("chain: " + Util.toBytesRef(arcAndState.chain, new BytesRef()));
            }
          }
          if (scratchArc.target != -1 && inCounts[scratchArc.target] != 1) {
            chain.length = 0;
          } else {
            chain.ints[0] = scratchArc.label;
            chain.length = 1;
          }
        }
        // TODO: instead of new Arc() we can re-use from
        // a by-depth array
        queue.add(new ArcAndState<T>(new Arc<T>().copyFrom(scratchArc), chain));
      } else if (!arcAndState.arc.isLast()) {
        // next
        readNextArc(arcAndState.arc);
        //System.out.println("  next label=" + (char) arcAndState.arc.label + " len=" + arcAndState.chain.length);
        if (arcAndState.chain.length != 0) {
          arcAndState.chain.ints[arcAndState.chain.length-1] = arcAndState.arc.label;
        }
      } else {
        if (arcAndState.chain.length > 1) {
          saved += arcAndState.chain.length-2;
          System.out.println("chain: " + Util.toBytesRef(arcAndState.chain, new BytesRef()).utf8ToString());
        }
        // pop
        //System.out.println("  pop");
        queue.remove(queue.size()-1);
        while(queue.size() > 0 && queue.get(queue.size()-1).arc.isLast()) {
          queue.remove(queue.size()-1);
        }
        if (queue.size() > 0) {
          final ArcAndState<T> arcAndState2 = queue.get(queue.size()-1);
          readNextArc(arcAndState2.arc);
          //System.out.println("  read next=" + (char) arcAndState2.arc.label + " queue=" + queue.size());
          assert arcAndState2.arc.label != END_LABEL;
          if (arcAndState2.chain.length != 0) {
            arcAndState2.chain.ints[arcAndState2.chain.length-1] = arcAndState2.arc.label;
          }
        }
      }
    }

    System.out.println("TOT saved " + saved);
  }
 */

  // Creates a packed FST
  private FST(INPUT_TYPE inputType, Outputs<T> outputs, int bytesPageBits) {
    version = VERSION_CURRENT;
    packed = true;
    this.inputType = inputType;
    bytesArray = null;
    bytes = new BytesStore(bytesPageBits);
    this.outputs = outputs;
  }

  /** Expert: creates an FST by packing this one.  This
   *  process requires substantial additional RAM (currently
   *  up to ~8 bytes per node depending on
   *  <code>acceptableOverheadRatio</code>), but then should
   *  produce a smaller FST.
   *
   *  <p>The implementation of this method uses ideas from
   *  <a target="_blank" href="http://www.cs.put.poznan.pl/dweiss/site/publications/download/fsacomp.pdf">Smaller Representation of Finite State Automata</a>,
   *  which describes techniques to reduce the size of a FST.
   *  However, this is not a strict implementation of the
   *  algorithms described in this paper.
   */
  FST<T> pack(Builder<T> builder, int minInCountDeref, int maxDerefNodes, float acceptableOverheadRatio) throws IOException {

    // NOTE: maxDerefNodes is intentionally int: we cannot
    // support > 2.1B deref nodes

    // TODO: other things to try
    //   - renumber the nodes to get more next / better locality?
    //   - allow multiple input labels on an arc, so
    //     singular chain of inputs can take one arc (on
    //     wikipedia terms this could save another ~6%)
    //   - in the ord case, the output '1' is presumably
    //     very common (after NO_OUTPUT)... maybe use a bit
    //     for it..?
    //   - use spare bits in flags.... for top few labels /
    //     outputs / targets

    if (nodeAddress == null) {
      throw new IllegalArgumentException("this FST was not built with willPackFST=true");
    }

    T NO_OUTPUT = outputs.getNoOutput();

    Arc<T> arc = new Arc<>();

    final BytesReader r = getBytesReader();

    final int topN = Math.min(maxDerefNodes, inCounts.size());

    // Find top nodes with highest number of incoming arcs:
    NodeQueue q = new NodeQueue(topN);

    // TODO: we could use more RAM efficient selection algo here...
    NodeAndInCount bottom = null;
    for(int node=0; node<inCounts.size(); node++) {
      if (inCounts.get(node) >= minInCountDeref) {
        if (bottom == null) {
          q.add(new NodeAndInCount(node, (int) inCounts.get(node)));
          if (q.size() == topN) {
            bottom = q.top();
          }
        } else if (inCounts.get(node) > bottom.count) {
          q.insertWithOverflow(new NodeAndInCount(node, (int) inCounts.get(node)));
        }
      }
    }

    // Free up RAM:
    inCounts = null;

    final Map<Integer,Integer> topNodeMap = new HashMap<>();
    for(int downTo=q.size()-1;downTo>=0;downTo--) {
      NodeAndInCount n = q.pop();
      topNodeMap.put(n.node, downTo);
      //System.out.println("map node=" + n.node + " inCount=" + n.count + " to newID=" + downTo);
    }

    // +1 because node ords start at 1 (0 is reserved as stop node):
    final GrowableWriter newNodeAddress = new GrowableWriter(
                                                             PackedInts.bitsRequired(builder.bytes.getPosition()), (int) (1 + builder.nodeCount), acceptableOverheadRatio);

    // Fill initial coarse guess:
    for(int node=1;node<=builder.nodeCount;node++) {
      newNodeAddress.set(node, 1 + builder.bytes.getPosition() - nodeAddress.get(node));
    }

    int absCount;
    int deltaCount;
    int topCount;
    int nextCount;

    FST<T> fst;

    // Iterate until we converge:
    while(true) {

      //System.out.println("\nITER");
      boolean changed = false;

      // for assert:
      boolean negDelta = false;

      fst = new FST<>(inputType, outputs, builder.bytes.getBlockBits());
      
      final BytesStore writer = fst.bytes;

      // Skip 0 byte since 0 is reserved target:
      writer.writeByte((byte) 0);

      absCount = deltaCount = topCount = nextCount = 0;

      int changedCount = 0;

      long addressError = 0;

      //int totWasted = 0;

      // Since we re-reverse the bytes, we now write the
      // nodes backwards, so that BIT_TARGET_NEXT is
      // unchanged:
      for(int node=(int) builder.nodeCount;node>=1;node--) {
        final long address = writer.getPosition();

        //System.out.println("  node: " + node + " address=" + address);
        if (address != newNodeAddress.get(node)) {
          addressError = address - newNodeAddress.get(node);
          //System.out.println("    change: " + (address - newNodeAddress[node]));
          changed = true;
          newNodeAddress.set(node, address);
          changedCount++;
        }

        int nodeArcCount = 0;
        int bytesPerArc = 0;

        boolean retry = false;

        // for assert:
        boolean anyNegDelta = false;

        // Retry loop: possibly iterate more than once, if
        // this is an array'd node and bytesPerArc changes:
        writeNode:
        while(true) { // retry writing this node

          //System.out.println("  cycle: retry");
          readFirstRealTargetArc(node, arc, r);

          final boolean useArcArray = arc.bytesPerArc != 0;
          if (useArcArray) {
            // Write false first arc:
            if (bytesPerArc == 0) {
              bytesPerArc = arc.bytesPerArc;
            }
            writer.writeByte(ARCS_AS_FIXED_ARRAY);
            writer.writeVInt(arc.numArcs);
            writer.writeVInt(bytesPerArc);
            //System.out.println("node " + node + ": " + arc.numArcs + " arcs");
          }

          int maxBytesPerArc = 0;
          //int wasted = 0;
          while(true) {  // iterate over all arcs for this node
            //System.out.println("    cycle next arc");

            final long arcStartPos = writer.getPosition();
            nodeArcCount++;

            byte flags = 0;

            if (arc.isLast()) {
              flags += BIT_LAST_ARC;
            }
            /*
            if (!useArcArray && nodeUpto < nodes.length-1 && arc.target == nodes[nodeUpto+1]) {
              flags += BIT_TARGET_NEXT;
            }
            */
            if (!useArcArray && node != 1 && arc.target == node-1) {
              flags += BIT_TARGET_NEXT;
              if (!retry) {
                nextCount++;
              }
            }
            if (arc.isFinal()) {
              flags += BIT_FINAL_ARC;
              if (arc.nextFinalOutput != NO_OUTPUT) {
                flags += BIT_ARC_HAS_FINAL_OUTPUT;
              }
            } else {
              assert arc.nextFinalOutput == NO_OUTPUT;
            }
            if (!targetHasArcs(arc)) {
              flags += BIT_STOP_NODE;
            }

            if (arc.output != NO_OUTPUT) {
              flags += BIT_ARC_HAS_OUTPUT;
            }

            final long absPtr;
            final boolean doWriteTarget = targetHasArcs(arc) && (flags & BIT_TARGET_NEXT) == 0;
            if (doWriteTarget) {

              final Integer ptr = topNodeMap.get(arc.target);
              if (ptr != null) {
                absPtr = ptr;
              } else {
                absPtr = topNodeMap.size() + newNodeAddress.get((int) arc.target) + addressError;
              }

              long delta = newNodeAddress.get((int) arc.target) + addressError - writer.getPosition() - 2;
              if (delta < 0) {
                //System.out.println("neg: " + delta);
                anyNegDelta = true;
                delta = 0;
              }

              if (delta < absPtr) {
                flags |= BIT_TARGET_DELTA;
              }
            } else {
              absPtr = 0;
            }

            assert flags != ARCS_AS_FIXED_ARRAY;
            writer.writeByte(flags);

            fst.writeLabel(writer, arc.label);

            if (arc.output != NO_OUTPUT) {
              outputs.write(arc.output, writer);
            }
            if (arc.nextFinalOutput != NO_OUTPUT) {
              outputs.writeFinalOutput(arc.nextFinalOutput, writer);
            }

            if (doWriteTarget) {

              long delta = newNodeAddress.get((int) arc.target) + addressError - writer.getPosition();
              if (delta < 0) {
                anyNegDelta = true;
                //System.out.println("neg: " + delta);
                delta = 0;
              }

              if (flag(flags, BIT_TARGET_DELTA)) {
                //System.out.println("        delta");
                writer.writeVLong(delta);
                if (!retry) {
                  deltaCount++;
                }
              } else {
                /*
                if (ptr != null) {
                  System.out.println("        deref");
                } else {
                  System.out.println("        abs");
                }
                */
                writer.writeVLong(absPtr);
                if (!retry) {
                  if (absPtr >= topNodeMap.size()) {
                    absCount++;
                  } else {
                    topCount++;
                  }
                }
              }
            }

            if (useArcArray) {
              final int arcBytes = (int) (writer.getPosition() - arcStartPos);
              //System.out.println("  " + arcBytes + " bytes");
              maxBytesPerArc = Math.max(maxBytesPerArc, arcBytes);
              // NOTE: this may in fact go "backwards", if
              // somehow (rarely, possibly never) we use
              // more bytesPerArc in this rewrite than the
              // incoming FST did... but in this case we
              // will retry (below) so it's OK to ovewrite
              // bytes:
              //wasted += bytesPerArc - arcBytes;
              writer.skipBytes((int) (arcStartPos + bytesPerArc - writer.getPosition()));
            }

            if (arc.isLast()) {
              break;
            }

            readNextRealArc(arc, r);
          }

          if (useArcArray) {
            if (maxBytesPerArc == bytesPerArc || (retry && maxBytesPerArc <= bytesPerArc)) {
              // converged
              //System.out.println("  bba=" + bytesPerArc + " wasted=" + wasted);
              //totWasted += wasted;
              break;
            }
          } else {
            break;
          }

          //System.out.println("  retry this node maxBytesPerArc=" + maxBytesPerArc + " vs " + bytesPerArc);

          // Retry:
          bytesPerArc = maxBytesPerArc;
          writer.truncate(address);
          nodeArcCount = 0;
          retry = true;
          anyNegDelta = false;
        }

        negDelta |= anyNegDelta;
      }

      if (!changed) {
        // We don't renumber the nodes (just reverse their
        // order) so nodes should only point forward to
        // other nodes because we only produce acyclic FSTs
        // w/ nodes only pointing "forwards":
        assert !negDelta;
        //System.out.println("TOT wasted=" + totWasted);
        // Converged!
        break;
      }
    }

    long maxAddress = 0;
    for (long key : topNodeMap.keySet()) {
      maxAddress = Math.max(maxAddress, newNodeAddress.get((int) key));
    }

    PackedInts.Mutable nodeRefToAddressIn = PackedInts.getMutable(topNodeMap.size(),
        PackedInts.bitsRequired(maxAddress), acceptableOverheadRatio);
    for(Map.Entry<Integer,Integer> ent : topNodeMap.entrySet()) {
      nodeRefToAddressIn.set(ent.getValue(), newNodeAddress.get(ent.getKey()));
    }
    fst.nodeRefToAddress = nodeRefToAddressIn;
    
    fst.startNode = newNodeAddress.get((int) startNode);
    //System.out.println("new startNode=" + fst.startNode + " old startNode=" + startNode);

    if (emptyOutput != null) {
      fst.setEmptyOutput(emptyOutput);
    }

    fst.bytes.finish();
    fst.cacheRootArcs();

    //final int size = fst.sizeInBytes();
    //System.out.println("nextCount=" + nextCount + " topCount=" + topCount + " deltaCount=" + deltaCount + " absCount=" + absCount);

    return fst;
  }

  private static class NodeAndInCount implements Comparable<NodeAndInCount> {
    final int node;
    final int count;

    public NodeAndInCount(int node, int count) {
      this.node = node;
      this.count = count;
    }
    
    @Override
    public int compareTo(NodeAndInCount other) {
      if (count > other.count) {
        return 1;
      } else if (count < other.count) {
        return -1;
      } else {
        // Tie-break: smaller node compares as greater than
        return other.node - node;
      }
    }
  }

  private static class NodeQueue extends PriorityQueue<NodeAndInCount> {
    public NodeQueue(int topN) {
      super(topN, false);
    }

    @Override
    public boolean lessThan(NodeAndInCount a, NodeAndInCount b) {
      final int cmp = a.compareTo(b);
      assert cmp != 0;
      return cmp < 0;
    }
  }
}
