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
package org.apache.lucene5_shaded.codecs.lucene41;

import org.apache.lucene5_shaded.codecs.BlockTermState;
import org.apache.lucene5_shaded.index.TermState;

/**
 * term state for Lucene 4.1 postings format
 * @deprecated only for reading old 4.x segments
 */
@Deprecated
final class IntBlockTermState extends BlockTermState {
  long docStartFP = 0;
  long posStartFP = 0;
  long payStartFP = 0;
  long skipOffset = -1;
  long lastPosBlockOffset = -1;
  // docid when there is a single pulsed posting, otherwise -1
  // freq is always implicitly totalTermFreq in this case.
  int singletonDocID = -1;

  @Override
  public IntBlockTermState clone() {
    IntBlockTermState other = new IntBlockTermState();
    other.copyFrom(this);
    return other;
  }

  @Override
  public void copyFrom(TermState _other) {
    super.copyFrom(_other);
    IntBlockTermState other = (IntBlockTermState) _other;
    docStartFP = other.docStartFP;
    posStartFP = other.posStartFP;
    payStartFP = other.payStartFP;
    lastPosBlockOffset = other.lastPosBlockOffset;
    skipOffset = other.skipOffset;
    singletonDocID = other.singletonDocID;
  }


  @Override
  public String toString() {
    return super.toString() + " docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset + " singletonDocID=" + singletonDocID;
  }
}