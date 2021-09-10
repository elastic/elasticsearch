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
package org.apache.lucene5_shaded.analysis.tokenattributes;


import org.apache.lucene5_shaded.util.Attribute;

/** Determines how many positions this
 *  token spans.  Very few analyzer components actually
 *  produce this attribute, and indexing ignores it, but
 *  it's useful to express the graph structure naturally
 *  produced by decompounding, word splitting/joining,
 *  synonym filtering, etc.
 *
 * <p>NOTE: this is optional, and most analyzers
 *  don't change the default value (1). */

public interface PositionLengthAttribute extends Attribute {
  /**
   * Set the position length of this Token.
   * <p>
   * The default value is one. 
   * @param positionLength how many positions this token
   *  spans. 
   * @throws IllegalArgumentException if <code>positionLength</code> 
   *         is zero or negative.
   * @see #getPositionLength()
   */
  public void setPositionLength(int positionLength);

  /** Returns the position length of this Token.
   * @see #setPositionLength
   */
  public int getPositionLength();
}

