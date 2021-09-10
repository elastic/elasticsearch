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
package org.apache.lucene5_shaded.analysis;


import java.io.IOException;
import java.io.Reader;

/**
 * Subclasses of CharFilter can be chained to filter a Reader
 * They can be used as {@link Reader} with additional offset
 * correction. {@link Tokenizer}s will automatically use {@link #correctOffset}
 * if a CharFilter subclass is used.
 * <p>
 * This class is abstract: at a minimum you must implement {@link #read(char[], int, int)},
 * transforming the input in some way from {@link #input}, and {@link #correct(int)}
 * to adjust the offsets to match the originals.
 * <p>
 * You can optionally provide more efficient implementations of additional methods 
 * like {@link #read()}, {@link #read(char[])}, {@link #read(java.nio.CharBuffer)},
 * but this is not required.
 * <p>
 * For examples and integration with {@link Analyzer}, see the 
 * {@link org.apache.lucene5_shaded.analysis Analysis package documentation}.
 */
// the way java.io.FilterReader should work!
public abstract class CharFilter extends Reader {
  /** 
   * The underlying character-input stream. 
   */
  protected final Reader input;

  /**
   * Create a new CharFilter wrapping the provided reader.
   * @param input a Reader, can also be a CharFilter for chaining.
   */
  public CharFilter(Reader input) {
    super(input);
    this.input = input;
  }
  
  /** 
   * Closes the underlying input stream.
   * <p>
   * <b>NOTE:</b> 
   * The default implementation closes the input Reader, so
   * be sure to call <code>super.close()</code> when overriding this method.
   */
  @Override
  public void close() throws IOException {
    input.close();
  }

  /**
   * Subclasses override to correct the current offset.
   *
   * @param currentOff current offset
   * @return corrected offset
   */
  protected abstract int correct(int currentOff);
  
  /**
   * Chains the corrected offset through the input
   * CharFilter(s).
   */
  public final int correctOffset(int currentOff) {
    final int corrected = correct(currentOff);
    return (input instanceof CharFilter) ? ((CharFilter) input).correctOffset(corrected) : corrected;
  }
}
