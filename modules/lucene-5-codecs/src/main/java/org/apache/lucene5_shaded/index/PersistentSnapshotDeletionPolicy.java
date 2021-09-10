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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.index.IndexWriterConfig.OpenMode;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.util.IOUtils;

/**
 * A {@link SnapshotDeletionPolicy} which adds a persistence layer so that
 * snapshots can be maintained across the life of an application. The snapshots
 * are persisted in a {@link Directory} and are committed as soon as
 * {@link #snapshot()} or {@link #release(IndexCommit)} is called.
 * <p>
 * <b>NOTE:</b> Sharing {@link PersistentSnapshotDeletionPolicy}s that write to
 * the same directory across {@link IndexWriter}s will corrupt snapshots. You
 * should make sure every {@link IndexWriter} has its own
 * {@link PersistentSnapshotDeletionPolicy} and that they all write to a
 * different {@link Directory}.  It is OK to use the same
 * Directory that holds the index.
 *
 * <p> This class adds a {@link #release(long)} method to
 * release commits from a previous snapshot's {@link IndexCommit#getGeneration}.
 *
 * @lucene.experimental
 */
public class PersistentSnapshotDeletionPolicy extends SnapshotDeletionPolicy {

  /** Prefix used for the save file. */
  public static final String SNAPSHOTS_PREFIX = "snapshots_";
  private static final int VERSION_START = 0;
  private static final int VERSION_CURRENT = VERSION_START;
  private static final String CODEC_NAME = "snapshots";

  // The index writer which maintains the snapshots metadata
  private long nextWriteGen;

  private final Directory dir;

  /**
   * {@link PersistentSnapshotDeletionPolicy} wraps another
   * {@link IndexDeletionPolicy} to enable flexible
   * snapshotting, passing {@link OpenMode#CREATE_OR_APPEND}
   * by default.
   * 
   * @param primary
   *          the {@link IndexDeletionPolicy} that is used on non-snapshotted
   *          commits. Snapshotted commits, by definition, are not deleted until
   *          explicitly released via {@link #release}.
   * @param dir
   *          the {@link Directory} which will be used to persist the snapshots
   *          information.
   */
  public PersistentSnapshotDeletionPolicy(IndexDeletionPolicy primary,
      Directory dir) throws IOException {
    this(primary, dir, OpenMode.CREATE_OR_APPEND);
  }

  /**
   * {@link PersistentSnapshotDeletionPolicy} wraps another
   * {@link IndexDeletionPolicy} to enable flexible snapshotting.
   * 
   * @param primary
   *          the {@link IndexDeletionPolicy} that is used on non-snapshotted
   *          commits. Snapshotted commits, by definition, are not deleted until
   *          explicitly released via {@link #release}.
   * @param dir
   *          the {@link Directory} which will be used to persist the snapshots
   *          information.
   * @param mode
   *          specifies whether a new index should be created, deleting all
   *          existing snapshots information (immediately), or open an existing
   *          index, initializing the class with the snapshots information.
   */
  public PersistentSnapshotDeletionPolicy(IndexDeletionPolicy primary,
      Directory dir, OpenMode mode) throws IOException {
    super(primary);

    this.dir = dir;

    if (mode == OpenMode.CREATE) {
      clearPriorSnapshots();
    }

    loadPriorSnapshots();

    if (mode == OpenMode.APPEND && nextWriteGen == 0) {
      throw new IllegalStateException("no snapshots stored in this directory");
    }
  }

  /**
   * Snapshots the last commit. Once this method returns, the
   * snapshot information is persisted in the directory.
   * 
   * @see SnapshotDeletionPolicy#snapshot
   */
  @Override
  public synchronized IndexCommit snapshot() throws IOException {
    IndexCommit ic = super.snapshot();
    boolean success = false;
    try {
      persist();
      success = true;
    } finally {
      if (!success) {
        try {
          super.release(ic);
        } catch (Exception e) {
          // Suppress so we keep throwing original exception
        }
      }
    }
    return ic;
  }

  /**
   * Deletes a snapshotted commit. Once this method returns, the snapshot
   * information is persisted in the directory.
   * 
   * @see SnapshotDeletionPolicy#release
   */
  @Override
  public synchronized void release(IndexCommit commit) throws IOException {
    super.release(commit);
    boolean success = false;
    try {
      persist();
      success = true;
    } finally {
      if (!success) {
        try {
          incRef(commit);
        } catch (Exception e) {
          // Suppress so we keep throwing original exception
        }
      }
    }
  }

  /**
   * Deletes a snapshotted commit by generation. Once this method returns, the snapshot
   * information is persisted in the directory.
   * 
   * @see IndexCommit#getGeneration
   * @see SnapshotDeletionPolicy#release
   */
  public synchronized void release(long gen) throws IOException {
    super.releaseGen(gen);
    persist();
  }

  synchronized private void persist() throws IOException {
    String fileName = SNAPSHOTS_PREFIX + nextWriteGen;
    IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT);
    boolean success = false;
    try {
      CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);   
      out.writeVInt(refCounts.size());
      for(Entry<Long,Integer> ent : refCounts.entrySet()) {
        out.writeVLong(ent.getKey());
        out.writeVInt(ent.getValue());
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
        IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
      } else {
        IOUtils.close(out);
      }
    }

    dir.sync(Collections.singletonList(fileName));
    
    if (nextWriteGen > 0) {
      String lastSaveFile = SNAPSHOTS_PREFIX + (nextWriteGen-1);
      // exception OK: likely it didn't exist
      IOUtils.deleteFilesIgnoringExceptions(dir, lastSaveFile);
    }

    nextWriteGen++;
  }

  private synchronized void clearPriorSnapshots() throws IOException {
    for(String file : dir.listAll()) {
      if (file.startsWith(SNAPSHOTS_PREFIX)) {
        dir.deleteFile(file);
      }
    }
  }

  /** Returns the file name the snapshots are currently
   *  saved to, or null if no snapshots have been saved. */
  public String getLastSaveFile() {
    if (nextWriteGen == 0) {
      return null;
    } else {
      return SNAPSHOTS_PREFIX + (nextWriteGen-1);
    }
  }

  /**
   * Reads the snapshots information from the given {@link Directory}. This
   * method can be used if the snapshots information is needed, however you
   * cannot instantiate the deletion policy (because e.g., some other process
   * keeps a lock on the snapshots directory).
   */
  private synchronized void loadPriorSnapshots() throws IOException {
    long genLoaded = -1;
    IOException ioe = null;
    List<String> snapshotFiles = new ArrayList<>();
    for(String file : dir.listAll()) {
      if (file.startsWith(SNAPSHOTS_PREFIX)) {
        long gen = Long.parseLong(file.substring(SNAPSHOTS_PREFIX.length()));
        if (genLoaded == -1 || gen > genLoaded) {
          snapshotFiles.add(file);
          Map<Long,Integer> m = new HashMap<>();
          IndexInput in = dir.openInput(file, IOContext.DEFAULT);
          try {
            CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_START);
            int count = in.readVInt();
            for(int i=0;i<count;i++) {
              long commitGen = in.readVLong();
              int refCount = in.readVInt();
              m.put(commitGen, refCount);
            }
          } catch (IOException ioe2) {
            // Save first exception & throw in the end
            if (ioe == null) {
              ioe = ioe2;
            }
          } finally {
            in.close();
          }

          genLoaded = gen;
          refCounts.clear();
          refCounts.putAll(m);
        }
      }
    }

    if (genLoaded == -1) {
      // Nothing was loaded...
      if (ioe != null) {
        // ... not for lack of trying:
        throw ioe;
      }
    } else { 
      if (snapshotFiles.size() > 1) {
        // Remove any broken / old snapshot files:
        String curFileName = SNAPSHOTS_PREFIX + genLoaded;
        for(String file : snapshotFiles) {
          if (!curFileName.equals(file)) {
            IOUtils.deleteFilesIgnoringExceptions(dir, file);
          }
        }
      }
      nextWriteGen = 1+genLoaded;
    }
  }
}
