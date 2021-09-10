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

TYPES = {8: "byte", 16: "short", 32: "int", 64: "long"}
MASKS = {8: " & 0xFFL", 16: " & 0xFFFFL", 32: " & 0xFFFFFFFFL", 64: ""}
CASTS = {8: "(byte) ", 16: "(short) ", 32: "(int) ", 64: ""}

if __name__ == '__main__':
  for bpv in TYPES.keys():
    type
    f = open("Direct%d.java" % bpv, 'w')
    f.write(HEADER)
    f.write("""/**
 * Direct wrapping of %d-bits values to a backing array.
 * @lucene.internal
 */\n""" % bpv)
    f.write("final class Direct%d extends PackedInts.MutableImpl {\n" % bpv)
    f.write("  final %s[] values;\n\n" % TYPES[bpv])

    f.write("  Direct%d(int valueCount) {\n" % bpv)
    f.write("    super(valueCount, %d);\n" % bpv)
    f.write("    values = new %s[valueCount];\n" % TYPES[bpv])
    f.write("  }\n\n")

    f.write("  Direct%d(int packedIntsVersion, DataInput in, int valueCount) throws IOException {\n" % bpv)
    f.write("    this(valueCount);\n")
    if bpv == 8:
      f.write("    in.readBytes(values, 0, valueCount);\n")
    else:
      f.write("    for (int i = 0; i < valueCount; ++i) {\n")
      f.write("      values[i] = in.read%s();\n" % TYPES[bpv].title())
      f.write("    }\n")
    if bpv != 64:
      f.write("    // because packed ints have not always been byte-aligned\n")
      f.write("    final int remaining = (int) (PackedInts.Format.PACKED.byteCount(packedIntsVersion, valueCount, %d) - %dL * valueCount);\n" % (bpv, bpv / 8))
      f.write("    for (int i = 0; i < remaining; ++i) {\n")
      f.write("      in.readByte();\n")
      f.write("    }\n")
    f.write("  }\n")

    f.write("""
  @Override
  public long get(final int index) {
    return values[index]%s;
  }

  @Override
  public void set(final int index, final long value) {
    values[index] = %s(value);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * RamUsageEstimator.NUM_BYTES_INT     // valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // values ref
        + RamUsageEstimator.sizeOf(values);
  }

  @Override
  public void clear() {
    Arrays.fill(values, %s0L);
  }
""" % (MASKS[bpv], CASTS[bpv], CASTS[bpv]))

    if bpv == 64:
      f.write("""
  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    System.arraycopy(values, index, arr, off, gets);
    return gets;
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    System.arraycopy(arr, off, values, index, sets);
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    Arrays.fill(values, fromIndex, toIndex, val);
  }
""")
    else:
      f.write("""
  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + gets; i < end; ++i, ++o) {
      arr[o] = values[i]%s;
    }
    return gets;
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + sets; i < end; ++i, ++o) {
      values[i] = %sarr[o];
    }
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    assert val == (val%s);
    Arrays.fill(values, fromIndex, toIndex, %sval);
  }
""" % (MASKS[bpv], CASTS[bpv], MASKS[bpv], CASTS[bpv]))

    f.write("}\n")

    f.close()
