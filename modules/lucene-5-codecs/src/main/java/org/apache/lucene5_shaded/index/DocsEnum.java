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
package org.apache.lucene5_shaded.index;

import java.io.IOException;

import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;

/** 
 * Iterates through the documents and term freqs.
 * NOTE: you must first call {@link #nextDoc} before using
 * any of the per-doc methods. 
 * @deprecated Use {@link PostingsEnum} instead.
 */
@Deprecated
public abstract class DocsEnum extends PostingsEnum {
  
  /**
   * Flag to pass to {@link TermsEnum#docs(Bits,DocsEnum,int)} if you don't
   * require term frequencies in the returned enum. When passed to
   * {@link TermsEnum#docsAndPositions(Bits,DocsAndPositionsEnum,int)} means
   * that no offsets and payloads will be returned.
   */
  public static final int FLAG_NONE = 0x0;

  /** Flag to pass to {@link TermsEnum#docs(Bits,DocsEnum,int)}
   *  if you require term frequencies in the returned enum. */
  public static final int FLAG_FREQS = 0x1;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocsEnum() {
  }

  @Override
  public int nextPosition() throws IOException {
    return -1;
  }

  @Override
  public int startOffset() throws IOException {
    return -1;
  }

  @Override
  public int endOffset() throws IOException {
    return -1;
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return null;
  }
}
