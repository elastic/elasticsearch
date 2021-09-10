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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene5_shaded.store.AlreadyClosedException;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.util.IOUtils;

final class StandardDirectoryReader extends DirectoryReader {

  final IndexWriter writer;
  final SegmentInfos segmentInfos;
  private final boolean applyAllDeletes;
  
  /** called only from static open() methods */
  StandardDirectoryReader(Directory directory, LeafReader[] readers, IndexWriter writer,
    SegmentInfos sis, boolean applyAllDeletes) throws IOException {
    super(directory, readers);
    this.writer = writer;
    this.segmentInfos = sis;
    this.applyAllDeletes = applyAllDeletes;
  }

  /** called from DirectoryReader.open(...) methods */
  static DirectoryReader open(final Directory directory, final IndexCommit commit) throws IOException {
    return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {
      @Override
      protected DirectoryReader doBody(String segmentFileName) throws IOException {
        SegmentInfos sis = SegmentInfos.readCommit(directory, segmentFileName);
        final SegmentReader[] readers = new SegmentReader[sis.size()];
        boolean success = false;
        try {
          for (int i = sis.size()-1; i >= 0; i--) {
            readers[i] = new SegmentReader(sis.info(i), IOContext.READ);
          }

          // This may throw CorruptIndexException if there are too many docs, so
          // it must be inside try clause so we close readers in that case:
          DirectoryReader reader = new StandardDirectoryReader(directory, readers, null, sis, false);
          success = true;

          return reader;
        } finally {
          if (success == false) {
            IOUtils.closeWhileHandlingException(readers);
          }
        }
      }
    }.run(commit);
  }

  /** Used by near real-time search */
  static DirectoryReader open(IndexWriter writer, SegmentInfos infos, boolean applyAllDeletes) throws IOException {
    // IndexWriter synchronizes externally before calling
    // us, which ensures infos will not change; so there's
    // no need to process segments in reverse order
    final int numSegments = infos.size();

    final List<SegmentReader> readers = new ArrayList<>(numSegments);
    final Directory dir = writer.getDirectory();

    final SegmentInfos segmentInfos = infos.clone();
    int infosUpto = 0;
    boolean success = false;
    try {
      for (int i = 0; i < numSegments; i++) {
        // NOTE: important that we use infos not
        // segmentInfos here, so that we are passing the
        // actual instance of SegmentInfoPerCommit in
        // IndexWriter's segmentInfos:
        final SegmentCommitInfo info = infos.info(i);
        assert info.info.dir == dir;
        final ReadersAndUpdates rld = writer.readerPool.get(info, true);
        try {
          final SegmentReader reader = rld.getReadOnlyClone(IOContext.READ);
          if (reader.numDocs() > 0 || writer.getKeepFullyDeletedSegments()) {
            // Steal the ref:
            readers.add(reader);
            infosUpto++;
          } else {
            reader.decRef();
            segmentInfos.remove(infosUpto);
          }
        } finally {
          writer.readerPool.release(rld);
        }
      }
      
      writer.incRefDeleter(segmentInfos);
      
      StandardDirectoryReader result = new StandardDirectoryReader(dir,
          readers.toArray(new SegmentReader[readers.size()]), writer,
          segmentInfos, applyAllDeletes);
      success = true;
      return result;
    } finally {
      if (!success) {
        for (SegmentReader r : readers) {
          try {
            r.decRef();
          } catch (Throwable th) {
            // ignore any exception that is thrown here to not mask any original
            // exception. 
          }
        }
      }
    }
  }

  /** This constructor is only used for {@link #doOpenIfChanged(SegmentInfos)} */
  private static DirectoryReader open(Directory directory, SegmentInfos infos, List<? extends LeafReader> oldReaders) throws IOException {

    // we put the old SegmentReaders in a map, that allows us
    // to lookup a reader using its segment name
    final Map<String,Integer> segmentReaders = (oldReaders == null ? Collections.<String,Integer>emptyMap() : new HashMap<String,Integer>(oldReaders.size()));

    if (oldReaders != null) {
      // create a Map SegmentName->SegmentReader
      for (int i = 0, c = oldReaders.size(); i < c; i++) {
        final SegmentReader sr = (SegmentReader) oldReaders.get(i);
        segmentReaders.put(sr.getSegmentName(), Integer.valueOf(i));
      }
    }
    
    SegmentReader[] newReaders = new SegmentReader[infos.size()];
    
    for (int i = infos.size() - 1; i>=0; i--) {
      SegmentCommitInfo commitInfo = infos.info(i);

      // find SegmentReader for this segment
      Integer oldReaderIndex = segmentReaders.get(commitInfo.info.name);
      SegmentReader oldReader;
      if (oldReaderIndex == null) {
        // this is a new segment, no old SegmentReader can be reused
        oldReader = null;
      } else {
        // there is an old reader for this segment - we'll try to reopen it
        oldReader = (SegmentReader) oldReaders.get(oldReaderIndex.intValue());
      }

      boolean success = false;
      try {
        SegmentReader newReader;
        if (oldReader == null || commitInfo.info.getUseCompoundFile() != oldReader.getSegmentInfo().info.getUseCompoundFile()) {

          // this is a new reader; in case we hit an exception we can decRef it safely
          newReader = new SegmentReader(commitInfo, IOContext.READ);
          newReaders[i] = newReader;
        } else {
          if (oldReader.getSegmentInfo().getDelGen() == commitInfo.getDelGen()
              && oldReader.getSegmentInfo().getFieldInfosGen() == commitInfo.getFieldInfosGen()) {
            // No change; this reader will be shared between
            // the old and the new one, so we must incRef
            // it:
            oldReader.incRef();
            newReaders[i] = oldReader;
          } else {
            // Steal the ref returned by SegmentReader ctor:
            assert commitInfo.info.dir == oldReader.getSegmentInfo().info.dir;

            // Make a best effort to detect when the app illegally "rm -rf" their
            // index while a reader was open, and then called openIfChanged:
            boolean illegalDocCountChange = commitInfo.info.maxDoc() != oldReader.getSegmentInfo().info.maxDoc();
            
            boolean hasNeitherDeletionsNorUpdates = commitInfo.hasDeletions()== false && commitInfo.hasFieldUpdates() == false;

            boolean deletesWereLost = commitInfo.getDelGen() == -1 && oldReader.getSegmentInfo().getDelGen() != -1;

            if (illegalDocCountChange || hasNeitherDeletionsNorUpdates || deletesWereLost) {
              throw new IllegalStateException("same segment " + commitInfo.info.name + " has invalid changes; likely you are re-opening a reader after illegally removing index files yourself and building a new index in their place.  Use IndexWriter.deleteAll or OpenMode.CREATE instead");
            }

            if (oldReader.getSegmentInfo().getDelGen() == commitInfo.getDelGen()) {
              // only DV updates
              newReaders[i] = new SegmentReader(commitInfo, oldReader, oldReader.getLiveDocs(), oldReader.numDocs());
            } else {
              // both DV and liveDocs have changed
              newReaders[i] = new SegmentReader(commitInfo, oldReader);
            }
          }
        }
        success = true;
      } finally {
        if (!success) {
          decRefWhileHandlingException(newReaders);
        }
      }
    }    
    return new StandardDirectoryReader(directory, newReaders, null, infos, false);
  }

  // TODO: move somewhere shared if it's useful elsewhere
  private static void decRefWhileHandlingException(SegmentReader[] readers) {
    for(SegmentReader reader : readers) {
      if (reader != null) {
        try {
          reader.decRef();
        } catch (Throwable t) {
          // Ignore so we keep throwing original exception
        }
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final String segmentsFile = segmentInfos.getSegmentsFileName();
    if (segmentsFile != null) {
      buffer.append(segmentsFile).append(":").append(segmentInfos.getVersion());
    }
    if (writer != null) {
      buffer.append(":nrt");
    }
    for (final LeafReader r : getSequentialSubReaders()) {
      buffer.append(' ');
      buffer.append(r);
    }
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  protected DirectoryReader doOpenIfChanged() throws IOException {
    return doOpenIfChanged((IndexCommit) null);
  }

  @Override
  protected DirectoryReader doOpenIfChanged(final IndexCommit commit) throws IOException {
    ensureOpen();

    // If we were obtained by writer.getReader(), re-ask the
    // writer to get a new reader.
    if (writer != null) {
      return doOpenFromWriter(commit);
    } else {
      return doOpenNoWriter(commit);
    }
  }

  @Override
  protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
    ensureOpen();
    if (writer == this.writer && applyAllDeletes == this.applyAllDeletes) {
      return doOpenFromWriter(null);
    } else {
      return writer.getReader(applyAllDeletes);
    }
  }

  private DirectoryReader doOpenFromWriter(IndexCommit commit) throws IOException {
    if (commit != null) {
      return doOpenFromCommit(commit);
    }

    if (writer.nrtIsCurrent(segmentInfos)) {
      return null;
    }

    DirectoryReader reader = writer.getReader(applyAllDeletes);

    // If in fact no changes took place, return null:
    if (reader.getVersion() == segmentInfos.getVersion()) {
      reader.decRef();
      return null;
    }

    return reader;
  }

  private DirectoryReader doOpenNoWriter(IndexCommit commit) throws IOException {

    if (commit == null) {
      if (isCurrent()) {
        return null;
      }
    } else {
      if (directory != commit.getDirectory()) {
        throw new IOException("the specified commit does not match the specified Directory");
      }
      if (segmentInfos != null && commit.getSegmentsFileName().equals(segmentInfos.getSegmentsFileName())) {
        return null;
      }
    }

    return doOpenFromCommit(commit);
  }

  private DirectoryReader doOpenFromCommit(IndexCommit commit) throws IOException {
    return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {
      @Override
      protected DirectoryReader doBody(String segmentFileName) throws IOException {
        final SegmentInfos infos = SegmentInfos.readCommit(directory, segmentFileName);
        return doOpenIfChanged(infos);
      }
    }.run(commit);
  }

  DirectoryReader doOpenIfChanged(SegmentInfos infos) throws IOException {
    return StandardDirectoryReader.open(directory, infos, getSequentialSubReaders());
  }

  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  @Override
  public boolean isCurrent() throws IOException {
    ensureOpen();
    if (writer == null || writer.isClosed()) {
      // Fully read the segments file: this ensures that it's
      // completely written so that if
      // IndexWriter.prepareCommit has been called (but not
      // yet commit), then the reader will still see itself as
      // current:
      SegmentInfos sis = SegmentInfos.readLatestCommit(directory);

      // we loaded SegmentInfos from the directory
      return sis.getVersion() == segmentInfos.getVersion();
    } else {
      return writer.nrtIsCurrent(segmentInfos);
    }
  }

  @Override
  protected void doClose() throws IOException {
    Throwable firstExc = null;
    for (final LeafReader r : getSequentialSubReaders()) {
      // try to close each reader, even if an exception is thrown
      try {
        r.decRef();
      } catch (Throwable t) {
        if (firstExc == null) {
          firstExc = t;
        }
      }
    }

    if (writer != null) {
      try {
        writer.decRefDeleter(segmentInfos);
      } catch (AlreadyClosedException ex) {
        // This is OK, it just means our original writer was
        // closed before we were, and this may leave some
        // un-referenced files in the index, which is
        // harmless.  The next time IW is opened on the
        // index, it will delete them.
      }
    }

    // throw the first exception
    IOUtils.reThrow(firstExc);
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    ensureOpen();
    return new ReaderCommit(this, segmentInfos, directory);
  }

  static final class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection<String> files;
    Directory dir;
    long generation;
    final Map<String,String> userData;
    private final int segmentCount;
    private final StandardDirectoryReader reader;

    ReaderCommit(StandardDirectoryReader reader, SegmentInfos infos, Directory dir) throws IOException {
      segmentsFileName = infos.getSegmentsFileName();
      this.dir = dir;
      userData = infos.getUserData();
      files = Collections.unmodifiableCollection(infos.files(true));
      generation = infos.getGeneration();
      segmentCount = infos.size();

      // NOTE: we intentionally do not incRef this!  Else we'd need to make IndexCommit Closeable...
      this.reader = reader;
    }

    @Override
    public String toString() {
      return "DirectoryReader.ReaderCommit(" + segmentsFileName + ")";
    }

    @Override
    public int getSegmentCount() {
      return segmentCount;
    }

    @Override
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    @Override
    public Collection<String> getFileNames() {
      return files;
    }

    @Override
    public Directory getDirectory() {
      return dir;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public boolean isDeleted() {
      return false;
    }

    @Override
    public Map<String,String> getUserData() {
      return userData;
    }

    @Override
    public void delete() {
      throw new UnsupportedOperationException("This IndexCommit does not support deletions");
    }

    @Override
    StandardDirectoryReader getReader() {
      return reader;
    }
  }
}
