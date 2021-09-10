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


/**
 * <p>A FlushInfo provides information required for a FLUSH context.
 *  It is used as part of an {@link IOContext} in case of FLUSH context.</p>
 */


public class FlushInfo {
  
  public final int numDocs;
  
  public final long estimatedSegmentSize;
  
  /**
   * <p>Creates a new {@link FlushInfo} instance from
   * the values required for a FLUSH {@link IOContext} context.
   * 
   * These values are only estimates and are not the actual values.
   * 
   */
  
  public FlushInfo(int numDocs, long estimatedSegmentSize) {
    this.numDocs = numDocs;
    this.estimatedSegmentSize = estimatedSegmentSize;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + (int) (estimatedSegmentSize ^ (estimatedSegmentSize >>> 32));
    result = prime * result + numDocs;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FlushInfo other = (FlushInfo) obj;
    if (estimatedSegmentSize != other.estimatedSegmentSize)
      return false;
    if (numDocs != other.numDocs)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "FlushInfo [numDocs=" + numDocs + ", estimatedSegmentSize="
        + estimatedSegmentSize + "]";
  }

}
