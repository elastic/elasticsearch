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
package org.apache.lucene5_shaded.store;

import java.io.IOException;


/** 
 * Extension of IndexInput, computing checksum as it goes. 
 * Callers can retrieve the checksum via {@link #getChecksum()}.
 */
public abstract class ChecksumIndexInput extends IndexInput {
  
  /** resourceDescription should be a non-null, opaque string
   *  describing this resource; it's returned from
   *  {@link #toString}. */
  protected ChecksumIndexInput(String resourceDescription) {
    super(resourceDescription);
  }

  /** Returns the current checksum value */
  public abstract long getChecksum() throws IOException;

  /**
   * {@inheritDoc}
   *
   * {@link ChecksumIndexInput} can only seek forward and seeks are expensive
   * since they imply to read bytes in-between the current position and the
   * target position in order to update the checksum.
   */
  @Override
  public void seek(long pos) throws IOException {
    final long curFP = getFilePointer();
    final long skip = pos - curFP;
    if (skip < 0) {
      throw new IllegalStateException(getClass() + " cannot seek backwards (pos=" + pos + " getFilePointer()=" + curFP + ")");
    }
    skipBytes(skip);
  }
}
