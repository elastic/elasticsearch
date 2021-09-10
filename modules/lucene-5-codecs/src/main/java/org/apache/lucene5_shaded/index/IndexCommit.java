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

import java.util.Collection;
import java.util.Map;
import java.io.IOException;

import org.apache.lucene5_shaded.store.Directory;

/**
 * <p>Expert: represents a single commit into an index as seen by the
 * {@link IndexDeletionPolicy} or {@link IndexReader}.</p>
 *
 * <p> Changes to the content of an index are made visible
 * only after the writer who made that change commits by
 * writing a new segments file
 * (<code>segments_N</code>). This point in time, when the
 * action of writing of a new segments file to the directory
 * is completed, is an index commit.</p>
 *
 * <p>Each index commit point has a unique segments file
 * associated with it. The segments file associated with a
 * later index commit point would have a larger N.</p>
 *
 * @lucene.experimental
*/

// TODO: this is now a poor name, because this class also represents a
// point-in-time view from an NRT reader
public abstract class IndexCommit implements Comparable<IndexCommit> {

  /**
   * Get the segments file (<code>segments_N</code>) associated 
   * with this commit point.
   */
  public abstract String getSegmentsFileName();

  /**
   * Returns all index files referenced by this commit point.
   */
  public abstract Collection<String> getFileNames() throws IOException;

  /**
   * Returns the {@link Directory} for the index.
   */
  public abstract Directory getDirectory();
  
  /**
   * Delete this commit point.  This only applies when using
   * the commit point in the context of IndexWriter's
   * IndexDeletionPolicy.
   * <p>
   * Upon calling this, the writer is notified that this commit 
   * point should be deleted. 
   * <p>
   * Decision that a commit-point should be deleted is taken by the {@link IndexDeletionPolicy} in effect
   * and therefore this should only be called by its {@link IndexDeletionPolicy#onInit onInit()} or 
   * {@link IndexDeletionPolicy#onCommit onCommit()} methods.
  */
  public abstract void delete();

  /** Returns true if this commit should be deleted; this is
   *  only used by {@link IndexWriter} after invoking the
   *  {@link IndexDeletionPolicy}. */
  public abstract boolean isDeleted();

  /** Returns number of segments referenced by this commit. */
  public abstract int getSegmentCount();

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected IndexCommit() {
  }

  /** Two IndexCommits are equal if both their Directory and versions are equal. */
  @Override
  public boolean equals(Object other) {
    if (other instanceof IndexCommit) {
      IndexCommit otherCommit = (IndexCommit) other;
      return otherCommit.getDirectory() == getDirectory() && otherCommit.getGeneration() == getGeneration();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return getDirectory().hashCode() + Long.valueOf(getGeneration()).hashCode();
  }

  /** Returns the generation (the _N in segments_N) for this
   *  IndexCommit */
  public abstract long getGeneration();

  /** Returns userData, previously passed to {@link
   *  IndexWriter#setCommitData(Map)} for this commit.  Map is
   *  {@code String -> String}. */
  public abstract Map<String,String> getUserData() throws IOException;
  
  @Override
  public int compareTo(IndexCommit commit) {
    if (getDirectory() != commit.getDirectory()) {
      throw new UnsupportedOperationException("cannot compare IndexCommits from different Directory instances");
    }

    long gen = getGeneration();
    long comgen = commit.getGeneration();
    return Long.compare(gen, comgen);
  }

  /** Package-private API for IndexWriter to init from a commit-point pulled from an NRT or non-NRT reader. */
  StandardDirectoryReader getReader() {
    return null;
  }
}
