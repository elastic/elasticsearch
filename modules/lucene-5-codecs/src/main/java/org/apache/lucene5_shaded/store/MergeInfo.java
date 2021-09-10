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
 * <p>A MergeInfo provides information required for a MERGE context.
 *  It is used as part of an {@link IOContext} in case of MERGE context.</p>
 */

public class MergeInfo {
  
  public final int totalMaxDoc;
  
  public final long estimatedMergeBytes;
  
  public final boolean isExternal;
  
  public final int mergeMaxNumSegments;
  

  /**
   * <p>Creates a new {@link MergeInfo} instance from
   * the values required for a MERGE {@link IOContext} context.
   * 
   * These values are only estimates and are not the actual values.
   * 
   */

  public MergeInfo(int totalMaxDoc, long estimatedMergeBytes, boolean isExternal, int mergeMaxNumSegments) {
    this.totalMaxDoc = totalMaxDoc;
    this.estimatedMergeBytes = estimatedMergeBytes;
    this.isExternal = isExternal;
    this.mergeMaxNumSegments = mergeMaxNumSegments;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + (int) (estimatedMergeBytes ^ (estimatedMergeBytes >>> 32));
    result = prime * result + (isExternal ? 1231 : 1237);
    result = prime * result + mergeMaxNumSegments;
    result = prime * result + totalMaxDoc;
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
    MergeInfo other = (MergeInfo) obj;
    if (estimatedMergeBytes != other.estimatedMergeBytes)
      return false;
    if (isExternal != other.isExternal)
      return false;
    if (mergeMaxNumSegments != other.mergeMaxNumSegments)
      return false;
    if (totalMaxDoc != other.totalMaxDoc)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "MergeInfo [totalMaxDoc=" + totalMaxDoc
        + ", estimatedMergeBytes=" + estimatedMergeBytes + ", isExternal="
        + isExternal + ", mergeMaxNumSegments=" + mergeMaxNumSegments + "]";
  }
}
