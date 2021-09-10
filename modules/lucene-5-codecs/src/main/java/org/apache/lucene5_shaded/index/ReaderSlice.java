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


/**
 * Subreader slice from a parent composite reader.
 *
 * @lucene.internal
 */
public final class ReaderSlice {

  /** Zero-length {@code ReaderSlice} array. */
  public static final ReaderSlice[] EMPTY_ARRAY = new ReaderSlice[0];

  /** Document ID this slice starts from. */
  public final int start;

  /** Number of documents in this slice. */
  public final int length;

  /** Sub-reader index for this slice. */
  public final int readerIndex;

  /** Sole constructor. */
  public ReaderSlice(int start, int length, int readerIndex) {
    this.start = start;
    this.length = length;
    this.readerIndex = readerIndex;
  }

  @Override
  public String toString() {
    return "slice start=" + start + " length=" + length + " readerIndex=" + readerIndex;
  }
}