#! /usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

HEADER = """// This file has been automatically generated, DO NOT EDIT

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
package org.apache.lucene5_shaded.util.packed;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

"""

TYPES = {8: "byte", 16: "short"}
MASKS = {8: " & 0xFFL", 16: " & 0xFFFFL", 32: " & 0xFFFFFFFFL", 64: ""}
CASTS = {8: "(byte) ", 16: "(short) ", 32: "(int) ", 64: ""}

if __name__ == '__main__':
  for bpv in TYPES.keys():
    type
    f = open("Packed%dThreeBlocks.java" % bpv, 'w')
    f.write(HEADER)
    f.write("""/**
 * Packs integers into 3 %ss (%d bits per value).
 * @lucene.internal
 */\n""" % (TYPES[bpv], bpv * 3))
    f.write("final class Packed%dThreeBlocks extends PackedInts.MutableImpl {\n" % bpv)
    f.write("  final %s[] blocks;\n\n" % TYPES[bpv])

    f.write("  public static final int MAX_SIZE = Integer.MAX_VALUE / 3;\n\n")

    f.write("  Packed%dThreeBlocks(int valueCount) {\n" % bpv)
    f.write("    super(valueCount, %d);\n" % (bpv * 3))
    f.write("    if (valueCount > MAX_SIZE) {\n")
    f.write("      throw new ArrayIndexOutOfBoundsException(\"MAX_SIZE exceeded\");\n")
    f.write("    }\n")
    f.write("    blocks = new %s[valueCount * 3];\n" % TYPES[bpv])
    f.write("  }\n\n")

    f.write("  Packed%dThreeBlocks(int packedIntsVersion, DataInput in, int valueCount) throws IOException {\n" % bpv)
    f.write("    this(valueCount);\n")
    if bpv == 8:
      f.write("    in.readBytes(blocks, 0, 3 * valueCount);\n")
    else:
      f.write("    for (int i = 0; i < 3 * valueCount; ++i) {\n")
      f.write("      blocks[i] = in.read%s();\n" % TYPES[bpv].title())
      f.write("    }\n")
    f.write("    // because packed ints have not always been byte-aligned\n")
    f.write("    final int remaining = (int) (PackedInts.Format.PACKED.byteCount(packedIntsVersion, valueCount, %d) - 3L * valueCount * %d);\n" %(3 * bpv, bpv / 8))
    f.write("    for (int i = 0; i < remaining; ++i) {\n")
    f.write("       in.readByte();\n")
    f.write("    }\n")
    f.write("  }\n")

    f.write("""
  @Override
  public long get(int index) {
    final int o = index * 3;
    return (blocks[o]%s) << %d | (blocks[o+1]%s) << %d | (blocks[o+2]%s);
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    for (int i = index * 3, end = (index + gets) * 3; i < end; i+=3) {
      arr[off++] = (blocks[i]%s) << %d | (blocks[i+1]%s) << %d | (blocks[i+2]%s);
    }
    return gets;
  }

  @Override
  public void set(int index, long value) {
    final int o = index * 3;
    blocks[o] = %s(value >>> %d);
    blocks[o+1] = %s(value >>> %d);
    blocks[o+2] = %svalue;
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    for (int i = off, o = index * 3, end = off + sets; i < end; ++i) {
      final long value = arr[i];
      blocks[o++] = %s(value >>> %d);
      blocks[o++] = %s(value >>> %d);
      blocks[o++] = %svalue;
    }
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    final %s block1 = %s(val >>> %d);
    final %s block2 = %s(val >>> %d);
    final %s block3 = %sval;
    for (int i = fromIndex * 3, end = toIndex * 3; i < end; i += 3) {
      blocks[i] = block1;
      blocks[i+1] = block2;
      blocks[i+2] = block3;
    }
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, %s0);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * RamUsageEstimator.NUM_BYTES_INT     // valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // blocks ref
        + RamUsageEstimator.sizeOf(blocks);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(bitsPerValue=" + bitsPerValue
        + ",size=" + size() + ",blocks=" + blocks.length + ")";
  }
}
""" % (MASKS[bpv], 2 * bpv, MASKS[bpv], bpv, MASKS[bpv], MASKS[bpv], 2 * bpv, MASKS[bpv], bpv, MASKS[bpv], CASTS[bpv], 2 * bpv, CASTS[bpv], bpv, CASTS[bpv], CASTS[bpv],
      2 * bpv, CASTS[bpv], bpv, CASTS[bpv], TYPES[bpv], CASTS[bpv], 2 * bpv, TYPES[bpv],
      CASTS[bpv], bpv, TYPES[bpv], CASTS[bpv], CASTS[bpv]))

    f.close()
