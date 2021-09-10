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
 * Encapsulates all required internal state to position the associated
 * {@link TermsEnum} without re-seeking.
 * 
 * @see TermsEnum#seekExact(org.apache.lucene5_shaded.util.BytesRef, TermState)
 * @see TermsEnum#termState()
 * @lucene.experimental
 */
public abstract class TermState implements Cloneable {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected TermState() {
  }

  /**
   * Copies the content of the given {@link TermState} to this instance
   * 
   * @param other
   *          the TermState to copy
   */
  public abstract void copyFrom(TermState other);

  @Override
  public TermState clone() {
    try {
      return (TermState)super.clone();
    } catch (CloneNotSupportedException cnse) {
      // should not happen
      throw new RuntimeException(cnse);
    }
  } 

  /** Returns true if this term is real (e.g., not an auto-prefix term).
   *  @lucene.internal */
  public boolean isRealTerm() {
    return true;
  }

  @Override
  public String toString() {
    return "TermState";
  }
}
