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
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.io.IOException;

import org.apache.lucene5_shaded.store.Directory;

/**
 * An {@link IndexDeletionPolicy} that wraps any other
 * {@link IndexDeletionPolicy} and adds the ability to hold and later release
 * snapshots of an index. While a snapshot is held, the {@link IndexWriter} will
 * not remove any files associated with it even if the index is otherwise being
 * actively, arbitrarily changed. Because we wrap another arbitrary
 * {@link IndexDeletionPolicy}, this gives you the freedom to continue using
 * whatever {@link IndexDeletionPolicy} you would normally want to use with your
 * index.
 * 
 * <p>
 * This class maintains all snapshots in-memory, and so the information is not
 * persisted and not protected against system failures. If persistence is
 * important, you can use {@link PersistentSnapshotDeletionPolicy}.
 * 
 * @lucene.experimental
 */
public class SnapshotDeletionPolicy extends IndexDeletionPolicy {

  /** Records how many snapshots are held against each
   *  commit generation */
  protected final Map<Long,Integer> refCounts = new HashMap<>();

  /** Used to map gen to IndexCommit. */
  protected final Map<Long,IndexCommit> indexCommits = new HashMap<>();

  /** Wrapped {@link IndexDeletionPolicy} */
  private final IndexDeletionPolicy primary;

  /** Most recently committed {@link IndexCommit}. */
  protected IndexCommit lastCommit;

  /** Used to detect misuse */
  private boolean initCalled;

  /** Sole constructor, taking the incoming {@link
   *  IndexDeletionPolicy} to wrap. */
  public SnapshotDeletionPolicy(IndexDeletionPolicy primary) {
    this.primary = primary;
  }

  @Override
  public synchronized void onCommit(List<? extends IndexCommit> commits)
      throws IOException {
    primary.onCommit(wrapCommits(commits));
    lastCommit = commits.get(commits.size() - 1);
  }

  @Override
  public synchronized void onInit(List<? extends IndexCommit> commits)
      throws IOException {
    initCalled = true;
    primary.onInit(wrapCommits(commits));
    for(IndexCommit commit : commits) {
      if (refCounts.containsKey(commit.getGeneration())) {
        indexCommits.put(commit.getGeneration(), commit);
      }
    }
    if (!commits.isEmpty()) {
      lastCommit = commits.get(commits.size() - 1);
    }
  }

  /**
   * Release a snapshotted commit.
   * 
   * @param commit
   *          the commit previously returned by {@link #snapshot}
   */
  public synchronized void release(IndexCommit commit) throws IOException {
    long gen = commit.getGeneration();
    releaseGen(gen);
  }

  /** Release a snapshot by generation. */
  protected void releaseGen(long gen) throws IOException {
    if (!initCalled) {
      throw new IllegalStateException("this instance is not being used by IndexWriter; be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
    }
    Integer refCount = refCounts.get(gen);
    if (refCount == null) {
      throw new IllegalArgumentException("commit gen=" + gen + " is not currently snapshotted");
    }
    int refCountInt = refCount.intValue();
    assert refCountInt > 0;
    refCountInt--;
    if (refCountInt == 0) {
      refCounts.remove(gen);
      indexCommits.remove(gen);
    } else {
      refCounts.put(gen, refCountInt);
    }
  }

  /** Increments the refCount for this {@link IndexCommit}. */
  protected synchronized void incRef(IndexCommit ic) {
    long gen = ic.getGeneration();
    Integer refCount = refCounts.get(gen);
    int refCountInt;
    if (refCount == null) {
      indexCommits.put(gen, lastCommit);
      refCountInt = 0;
    } else {
      refCountInt = refCount.intValue();
    }
    refCounts.put(gen, refCountInt+1);
  }

  /**
   * Snapshots the last commit and returns it. Once a commit is 'snapshotted,' it is protected
   * from deletion (as long as this {@link IndexDeletionPolicy} is used). The
   * snapshot can be removed by calling {@link #release(IndexCommit)} followed
   * by a call to {@link IndexWriter#deleteUnusedFiles()}.
   *
   * <p>
   * <b>NOTE:</b> while the snapshot is held, the files it references will not
   * be deleted, which will consume additional disk space in your index. If you
   * take a snapshot at a particularly bad time (say just before you call
   * forceMerge) then in the worst case this could consume an extra 1X of your
   * total index size, until you release the snapshot.
   * 
   * @throws IllegalStateException
   *           if this index does not have any commits yet
   * @return the {@link IndexCommit} that was snapshotted.
   */
  public synchronized IndexCommit snapshot() throws IOException {
    if (!initCalled) {
      throw new IllegalStateException("this instance is not being used by IndexWriter; be sure to use the instance returned from writer.getConfig().getIndexDeletionPolicy()");
    }
    if (lastCommit == null) {
      // No commit yet, eg this is a new IndexWriter:
      throw new IllegalStateException("No index commit to snapshot");
    }

    incRef(lastCommit);

    return lastCommit;
  }

  /** Returns all IndexCommits held by at least one snapshot. */
  public synchronized List<IndexCommit> getSnapshots() {
    return new ArrayList<>(indexCommits.values());
  }

  /** Returns the total number of snapshots currently held. */
  public synchronized int getSnapshotCount() {
    int total = 0;
    for(Integer refCount : refCounts.values()) {
      total += refCount.intValue();
    }

    return total;
  }

  /** Retrieve an {@link IndexCommit} from its generation;
   *  returns null if this IndexCommit is not currently
   *  snapshotted  */
  public synchronized IndexCommit getIndexCommit(long gen) {
    return indexCommits.get(gen);
  }

  /** Wraps each {@link IndexCommit} as a {@link
   *  SnapshotCommitPoint}. */
  private List<IndexCommit> wrapCommits(List<? extends IndexCommit> commits) {
    List<IndexCommit> wrappedCommits = new ArrayList<>(commits.size());
    for (IndexCommit ic : commits) {
      wrappedCommits.add(new SnapshotCommitPoint(ic));
    }
    return wrappedCommits;
  }

  /** Wraps a provided {@link IndexCommit} and prevents it
   *  from being deleted. */
  private class SnapshotCommitPoint extends IndexCommit {

    /** The {@link IndexCommit} we are preventing from deletion. */
    protected IndexCommit cp;

    /** Creates a {@code SnapshotCommitPoint} wrapping the provided
     *  {@link IndexCommit}. */
    protected SnapshotCommitPoint(IndexCommit cp) {
      this.cp = cp;
    }

    @Override
    public String toString() {
      return "SnapshotDeletionPolicy.SnapshotCommitPoint(" + cp + ")";
    }

    @Override
    public void delete() {
      synchronized (SnapshotDeletionPolicy.this) {
        // Suppress the delete request if this commit point is
        // currently snapshotted.
        if (!refCounts.containsKey(cp.getGeneration())) {
          cp.delete();
        }
      }
    }

    @Override
    public Directory getDirectory() {
      return cp.getDirectory();
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
      return cp.getFileNames();
    }

    @Override
    public long getGeneration() {
      return cp.getGeneration();
    }

    @Override
    public String getSegmentsFileName() {
      return cp.getSegmentsFileName();
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
      return cp.getUserData();
    }

    @Override
    public boolean isDeleted() {
      return cp.isDeleted();
    }

    @Override
    public int getSegmentCount() {
      return cp.getSegmentCount();
    }
  }
}
