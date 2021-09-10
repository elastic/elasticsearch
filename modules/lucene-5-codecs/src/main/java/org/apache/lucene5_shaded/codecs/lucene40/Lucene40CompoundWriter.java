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
package org.apache.lucene5_shaded.codecs.lucene40;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.store.AlreadyClosedException;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.FlushInfo;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.util.IOUtils;

/**
 * Combines multiple files into a single compound file.
 * @deprecated only for testing
 */
@Deprecated
final class Lucene40CompoundWriter implements Closeable{

  private static final class FileEntry {
    /** source file */
    String file;
    long length;
    /** temporary holder for the start of this file's data section */
    long offset;
    /** the directory which contains the file. */
    Directory dir;
  }

  // versioning for the .cfs file
  static final String DATA_CODEC = "CompoundFileWriterData";
  static final int VERSION_START = 0;
  static final int VERSION_CHECKSUM = 1;
  static final int VERSION_CURRENT = VERSION_CHECKSUM;

  // versioning for the .cfe file
  static final String ENTRY_CODEC = "CompoundFileWriterEntries";

  private final Directory directory;
  private final Map<String, FileEntry> entries = new HashMap<>();
  private final Set<String> seenIDs = new HashSet<>();
  // all entries that are written to a sep. file but not yet moved into CFS
  private final Queue<FileEntry> pendingEntries = new LinkedList<>();
  private boolean closed = false;
  private IndexOutput dataOut;
  private final AtomicBoolean outputTaken = new AtomicBoolean(false);
  final String entryTableName;
  final String dataFileName;
  
  // preserve the IOContext we were originally passed
  // previously this was not also passed to the .CFE
  private final IOContext context;

  /**
   * Create the compound stream in the specified file. The file name is the
   * entire name (no extensions are added).
   * 
   * @throws NullPointerException
   *           if <code>dir</code> or <code>name</code> is null
   */
  Lucene40CompoundWriter(Directory dir, String name, IOContext context) {
    if (dir == null)
      throw new NullPointerException("directory cannot be null");
    if (name == null)
      throw new NullPointerException("name cannot be null");
    directory = dir;
    entryTableName = IndexFileNames.segmentFileName(
        IndexFileNames.stripExtension(name), "",
        Lucene40CompoundFormat.COMPOUND_FILE_ENTRIES_EXTENSION);
    dataFileName = name;
    this.context = context;
  }
  
  private synchronized IndexOutput getOutput(IOContext context) throws IOException {
    if (dataOut == null) {
      boolean success = false;
      try {
        dataOut = directory.createOutput(dataFileName, this.context);
        CodecUtil.writeHeader(dataOut, DATA_CODEC, VERSION_CURRENT);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(dataOut);
        }
      }
    } 
    return dataOut;
  }

  /** Returns the directory of the compound file. */
  Directory getDirectory() {
    return directory;
  }

  /** Returns the name of the compound file. */
  String getName() {
    return dataFileName;
  }

  /**
   * Closes all resources and writes the entry table
   * 
   * @throws IllegalStateException
   *           if close() had been called before or if no file has been added to
   *           this object
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    IndexOutput entryTableOut = null;
    // TODO this code should clean up after itself
    // (remove partial .cfs/.cfe)
    boolean success = false;
    try {
      if (!pendingEntries.isEmpty() || outputTaken.get()) {
        throw new IllegalStateException("CFS has pending open files");
      }
      closed = true;
      getOutput(this.context);
      assert dataOut != null;
      CodecUtil.writeFooter(dataOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(dataOut);
      } else {
        IOUtils.closeWhileHandlingException(dataOut);
      }
    }
    success = false;
    try {
      entryTableOut = directory.createOutput(entryTableName, this.context);
      writeEntryTable(entries.values(), entryTableOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(entryTableOut);
      } else {
        IOUtils.closeWhileHandlingException(entryTableOut);
      }
    }
  }

  private final void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("CFS Directory is already closed");
    }
  }

  /**
   * Copy the contents of the file with specified extension into the provided
   * output stream.
   */
  private final long copyFileEntry(IndexOutput dataOut, FileEntry fileEntry)
      throws IOException {
    final IndexInput is = fileEntry.dir.openInput(fileEntry.file, IOContext.READONCE);
    boolean success = false;
    try {
      final long startPtr = dataOut.getFilePointer();
      final long length = fileEntry.length;
      dataOut.copyBytes(is, length);
      // Verify that the output length diff is equal to original file
      long endPtr = dataOut.getFilePointer();
      long diff = endPtr - startPtr;
      if (diff != length)
        throw new IOException("Difference in the output file offsets " + diff
            + " does not match the original file length " + length);
      fileEntry.offset = startPtr;
      success = true;
      return length;
    } finally {
      if (success) {
        IOUtils.close(is);
        // copy successful - delete file
        // if we can't we rely on IFD to pick up and retry
        IOUtils.deleteFilesIgnoringExceptions(fileEntry.dir, fileEntry.file);
      } else {
        IOUtils.closeWhileHandlingException(is);
      }
    }
  }

  protected void writeEntryTable(Collection<FileEntry> entries,
      IndexOutput entryOut) throws IOException {
    CodecUtil.writeHeader(entryOut, ENTRY_CODEC, VERSION_CURRENT);
    entryOut.writeVInt(entries.size());
    for (FileEntry fe : entries) {
      entryOut.writeString(IndexFileNames.stripSegmentName(fe.file));
      entryOut.writeLong(fe.offset);
      entryOut.writeLong(fe.length);
    }
    CodecUtil.writeFooter(entryOut);
  }

  IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    boolean success = false;
    boolean outputLocked = false;
    try {
      assert name != null : "name must not be null";
      if (entries.containsKey(name)) {
        throw new IllegalArgumentException("File " + name + " already exists");
      }
      final FileEntry entry = new FileEntry();
      entry.file = name;
      entries.put(name, entry);
      final String id = IndexFileNames.stripSegmentName(name);
      assert !seenIDs.contains(id): "file=\"" + name + "\" maps to id=\"" + id + "\", which was already written";
      seenIDs.add(id);
      final DirectCFSIndexOutput out;

      if ((outputLocked = outputTaken.compareAndSet(false, true))) {
        out = new DirectCFSIndexOutput(getOutput(this.context), entry, false);
      } else {
        entry.dir = this.directory;
        out = new DirectCFSIndexOutput(directory.createOutput(name, this.context), entry,
            true);
      }
      success = true;
      return out;
    } finally {
      if (!success) {
        entries.remove(name);
        if (outputLocked) { // release the output lock if not successful
          assert outputTaken.get();
          releaseOutputLock();
        }
      }
    }
  }

  final void releaseOutputLock() {
    outputTaken.compareAndSet(true, false);
  }

  private final void prunePendingEntries() throws IOException {
    // claim the output and copy all pending files in
    if (outputTaken.compareAndSet(false, true)) {
      try {
        while (!pendingEntries.isEmpty()) {
          FileEntry entry = pendingEntries.poll();
          copyFileEntry(getOutput(this.context), entry);
          entries.put(entry.file, entry);
        }
      } finally {
        final boolean compareAndSet = outputTaken.compareAndSet(true, false);
        assert compareAndSet;
      }
    }
  }

  long fileLength(String name) throws IOException {
    FileEntry fileEntry = entries.get(name);
    if (fileEntry == null) {
      throw new FileNotFoundException(name + " does not exist");
    }
    return fileEntry.length;
  }

  boolean fileExists(String name) {
    return entries.containsKey(name);
  }

  String[] listAll() {
    return entries.keySet().toArray(new String[0]);
  }

  private final class DirectCFSIndexOutput extends IndexOutput {
    private final IndexOutput delegate;
    private final long offset;
    private boolean closed;
    private FileEntry entry;
    private long writtenBytes;
    private final boolean isSeparate;

    DirectCFSIndexOutput(IndexOutput delegate, FileEntry entry,
        boolean isSeparate) {
      super("DirectCFSIndexOutput("+delegate.toString()+",entry=\""+entry.toString()+"\",isSeparate=\""+isSeparate+")");
      this.delegate = delegate;
      this.entry = entry;
      entry.offset = offset = delegate.getFilePointer();
      this.isSeparate = isSeparate;

    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        entry.length = writtenBytes;
        if (isSeparate) {
          delegate.close();
          // we are a separate file - push into the pending entries
          pendingEntries.add(entry);
        } else {
          // we have been written into the CFS directly - release the lock
          releaseOutputLock();
        }
        // now prune all pending entries and push them into the CFS
        prunePendingEntries();
      }
    }

    @Override
    public long getFilePointer() {
      return delegate.getFilePointer() - offset;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      assert !closed;
      writtenBytes++;
      delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      assert !closed;
      writtenBytes += length;
      delegate.writeBytes(b, offset, length);
    }

    @Override
    public long getChecksum() throws IOException {
      return delegate.getChecksum();
    }
  }

}
