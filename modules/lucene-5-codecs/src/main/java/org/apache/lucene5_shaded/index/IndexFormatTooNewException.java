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
 * an index that is newer than this Lucene version.
 */
public class IndexFormatTooNewException extends IOException {

  private final String resourceDescription;
  private final int version;
  private final int minVersion;
  private final int maxVersion;

  /** Creates an {@code IndexFormatTooNewException}
   *
   *  @param resourceDescription describes the file that was too new
   *  @param version the version of the file that was too new
   *  @param minVersion the minimum version accepted
   *  @param maxVersion the maximum version accepted
   *
   * @lucene.internal */
  public IndexFormatTooNewException(String resourceDescription, int version, int minVersion, int maxVersion) {
    super("Format version is not supported (resource " + resourceDescription + "): "
        + version + " (needs to be between " + minVersion + " and " + maxVersion + ")");
    this.resourceDescription = resourceDescription;
    this.version = version;
    this.minVersion = minVersion;
    this.maxVersion = maxVersion;
  }

  /** Creates an {@code IndexFormatTooNewException}
   *
   *  @param in the open file that's too new
   *  @param version the version of the file that was too new
   *  @param minVersion the minimum version accepted
   *  @param maxVersion the maximum version accepted
   *
   * @lucene.internal */
  public IndexFormatTooNewException(DataInput in, int version, int minVersion, int maxVersion) {
    this(Objects.toString(in), version, minVersion, maxVersion);
  }

  /**
   * Returns a description of the file that was too new
   */
  public String getResourceDescription() {
    return resourceDescription;
  }

  /**
   * Returns the version of the file that was too new
   */
  public int getVersion() {
    return version;
  }

  /**
   * Returns the maximum version accepted
   */
  public int getMaxVersion() {
    return maxVersion;
  }

  /**
   * Returns the minimum version accepted
   */
  public int getMinVersion() {
    return minVersion;
  }
}
