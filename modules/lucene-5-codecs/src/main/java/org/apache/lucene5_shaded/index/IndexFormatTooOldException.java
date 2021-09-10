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
import java.util.Objects;

import org.apache.lucene5_shaded.store.DataInput;

/**
 * This exception is thrown when Lucene detects
 * an index that is too old for this Lucene version
 */
public class IndexFormatTooOldException extends IOException {

  private final String resourceDescription;
  private final String reason;
  private final Integer version;
  private final Integer minVersion;
  private final Integer maxVersion;


  /** Creates an {@code IndexFormatTooOldException}.
   *
   *  @param resourceDescription describes the file that was too old
   *  @param reason the reason for this exception if the version is not available
   *
   * @lucene.internal */
  public IndexFormatTooOldException(String resourceDescription, String reason) {
    super("Format version is not supported (resource " + resourceDescription + "): " +
        reason + ". This version of Lucene only supports indexes created with release 4.0 and later.");
    this.resourceDescription = resourceDescription;
    this.reason = reason;
    this.version = null;
    this.minVersion = null;
    this.maxVersion = null;

  }

  /** Creates an {@code IndexFormatTooOldException}.
   *
   *  @param in the open file that's too old
   *  @param reason the reason for this exception if the version is not available
   *
   * @lucene.internal */
  public IndexFormatTooOldException(DataInput in, String reason) {
    this(Objects.toString(in), reason);
  }

  /** Creates an {@code IndexFormatTooOldException}.
   *
   *  @param resourceDescription describes the file that was too old
   *  @param version the version of the file that was too old
   *  @param minVersion the minimum version accepted
   *  @param maxVersion the maximum version accepted
   * 
   * @lucene.internal */
  public IndexFormatTooOldException(String resourceDescription, int version, int minVersion, int maxVersion) {
    super("Format version is not supported (resource " + resourceDescription + "): " +
        version + " (needs to be between " + minVersion + " and " + maxVersion +
        "). This version of Lucene only supports indexes created with release 4.0 and later.");
    this.resourceDescription = resourceDescription;
    this.version = version;
    this.minVersion = minVersion;
    this.maxVersion = maxVersion;
    this.reason = null;
  }

  /** Creates an {@code IndexFormatTooOldException}.
   *
   *  @param in the open file that's too old
   *  @param version the version of the file that was too old
   *  @param minVersion the minimum version accepted
   *  @param maxVersion the maximum version accepted
   *
   * @lucene.internal */
  public IndexFormatTooOldException(DataInput in, int version, int minVersion, int maxVersion) {
    this(Objects.toString(in), version, minVersion, maxVersion);
  }

  /**
   * Returns a description of the file that was too old
   */
  public String getResourceDescription() {
    return resourceDescription;
  }

  /**
   * Returns an optional reason for this exception if the version information was not available. Otherwise <code>null</code>
   */
  public String getReason() {
    return reason;
  }

  /**
   * Returns the version of the file that was too old.
   * This method will return <code>null</code> if an alternative {@link #getReason()}
   * is provided.
   */
  public Integer getVersion() {
    return version;
  }

  /**
   * Returns the maximum version accepted.
   * This method will return <code>null</code> if an alternative {@link #getReason()}
   * is provided.
   */
  public Integer getMaxVersion() {
    return maxVersion;
  }

  /**
   * Returns the minimum version accepted
   * This method will return <code>null</code> if an alternative {@link #getReason()}
   * is provided.
   */
  public Integer getMinVersion() {
    return minVersion;
  }
}
