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


import java.io.IOException;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.util.CharsRef;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * An FST {@link Outputs} implementation where each output
 * is a sequence of characters.
 *
 * @lucene.experimental
 */

public final class CharSequenceOutputs extends Outputs<CharsRef> {

  private final static CharsRef NO_OUTPUT = new CharsRef();
  private final static CharSequenceOutputs singleton = new CharSequenceOutputs();

  private CharSequenceOutputs() {
  }

  public static CharSequenceOutputs getSingleton() {
    return singleton;
  }

  @Override
  public CharsRef common(CharsRef output1, CharsRef output2) {
    assert output1 != null;
    assert output2 != null;

    int pos1 = output1.offset;
    int pos2 = output2.offset;
    int stopAt1 = pos1 + Math.min(output1.length, output2.length);
    while(pos1 < stopAt1) {
      if (output1.chars[pos1] != output2.chars[pos2]) {
        break;
      }
      pos1++;
      pos2++;
    }

    if (pos1 == output1.offset) {
      // no common prefix
      return NO_OUTPUT;
    } else if (pos1 == output1.offset + output1.length) {
      // output1 is a prefix of output2
      return output1;
    } else if (pos2 == output2.offset + output2.length) {
      // output2 is a prefix of output1
      return output2;
    } else {
      return new CharsRef(output1.chars, output1.offset, pos1-output1.offset);
    }
  }

  @Override
  public CharsRef subtract(CharsRef output, CharsRef inc) {
    assert output != null;
    assert inc != null;
    if (inc == NO_OUTPUT) {
      // no prefix removed
      return output;
    } else if (inc.length == output.length) {
      // entire output removed
      return NO_OUTPUT;
    } else {
      assert inc.length < output.length: "inc.length=" + inc.length + " vs output.length=" + output.length;
      assert inc.length > 0;
      return new CharsRef(output.chars, output.offset + inc.length, output.length-inc.length);
    }
  }

  @Override
  public CharsRef add(CharsRef prefix, CharsRef output) {
    assert prefix != null;
    assert output != null;
    if (prefix == NO_OUTPUT) {
      return output;
    } else if (output == NO_OUTPUT) {
      return prefix;
    } else {
      assert prefix.length > 0;
      assert output.length > 0;
      CharsRef result = new CharsRef(prefix.length + output.length);
      System.arraycopy(prefix.chars, prefix.offset, result.chars, 0, prefix.length);
      System.arraycopy(output.chars, output.offset, result.chars, prefix.length, output.length);
      result.length = prefix.length + output.length;
      return result;
    }
  }

  @Override
  public void write(CharsRef prefix, DataOutput out) throws IOException {
    assert prefix != null;
    out.writeVInt(prefix.length);
    // TODO: maybe UTF8?
    for(int idx=0;idx<prefix.length;idx++) {
      out.writeVInt(prefix.chars[prefix.offset+idx]);
    }
  }

  @Override
  public CharsRef read(DataInput in) throws IOException {
    final int len = in.readVInt();
    if (len == 0) {
      return NO_OUTPUT;
    } else {
      final CharsRef output = new CharsRef(len);
      for(int idx=0;idx<len;idx++) {
        output.chars[idx] = (char) in.readVInt();
      }
      output.length = len;
      return output;
    }
  }
  
  @Override
  public void skipOutput(DataInput in) throws IOException {
    final int len = in.readVInt();
    for(int idx=0;idx<len;idx++) {
      in.readVInt();
    }
  }

  @Override
  public CharsRef getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(CharsRef output) {
    return output.toString();
  }

  private static final long BASE_NUM_BYTES = RamUsageEstimator.shallowSizeOf(NO_OUTPUT);

  @Override
  public long ramBytesUsed(CharsRef output) {
    return BASE_NUM_BYTES + RamUsageEstimator.sizeOf(output.chars);
  }
}
