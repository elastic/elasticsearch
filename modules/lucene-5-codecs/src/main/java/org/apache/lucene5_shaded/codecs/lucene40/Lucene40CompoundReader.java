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

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.store.BufferedIndexInput;
import org.apache.lucene5_shaded.store.ChecksumIndexInput;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.store.Lock;
import org.apache.lucene5_shaded.util.IOUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Lucene 4.x compound file format
 * @deprecated only for reading 4.x segments
 */
@Deprecated
final class Lucene40CompoundReader extends Directory {
  
  // TODO: would be great to move this read-write stuff out of here into test.

  /** Offset/Length for a slice inside of a compound file */
  public static final class FileEntry {
    long offset;
    long length;
  }
  
  private final Directory directory;
  private final String fileName;
  protected final int readBufferSize;  
  private final Map<String,FileEntry> entries;
  private final boolean openForWrite;
  private static final Map<String,FileEntry> SENTINEL = Collections.emptyMap();
  private final Lucene40CompoundWriter writer;
  private final IndexInput handle;
  private int version;
  private boolean isOpen;
  
  /**
   * Create a new CompoundFileDirectory.
   */
  public Lucene40CompoundReader(Directory directory, String fileName, IOContext context, boolean openForWrite) throws IOException {
    this.directory = directory;
    this.fileName = fileName;
    this.readBufferSize = BufferedIndexInput.bufferSize(context);
    this.isOpen = false;
    this.openForWrite = openForWrite;
    if (!openForWrite) {
      boolean success = false;
      handle = directory.openInput(fileName, context);
      try {
        this.entries = readEntries(directory, fileName);
        if (version >= Lucene40CompoundWriter.VERSION_CHECKSUM) {
          CodecUtil.checkHeader(handle, Lucene40CompoundWriter.DATA_CODEC, version, version);
          // NOTE: data file is too costly to verify checksum against all the bytes on open,
          // but for now we at least verify proper structure of the checksum footer: which looks
          // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
          // such as file truncation.
          CodecUtil.retrieveChecksum(handle);
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(handle);
        }
      }
      this.isOpen = true;
      writer = null;
    } else {
      assert !(directory instanceof Lucene40CompoundReader) : "compound file inside of compound file: " + fileName;
      this.entries = SENTINEL;
      this.isOpen = true;
      writer = new Lucene40CompoundWriter(directory, fileName, context);
      handle = null;
    }
  }

  /** Helper method that reads CFS entries from an input stream */
  private final Map<String, FileEntry> readEntries(Directory dir, String name) throws IOException {
    ChecksumIndexInput entriesStream = null;
    Map<String,FileEntry> mapping = null;
    boolean success = false;
    try {
      final String entriesFileName = IndexFileNames.segmentFileName(
                                            IndexFileNames.stripExtension(name), "",
                                             Lucene40CompoundFormat.COMPOUND_FILE_ENTRIES_EXTENSION);
      entriesStream = dir.openChecksumInput(entriesFileName, IOContext.READONCE);
      version = CodecUtil.checkHeader(entriesStream, Lucene40CompoundWriter.ENTRY_CODEC, Lucene40CompoundWriter.VERSION_START, Lucene40CompoundWriter.VERSION_CURRENT);
      final int numEntries = entriesStream.readVInt();
      mapping = new HashMap<>(numEntries);
      for (int i = 0; i < numEntries; i++) {
        final FileEntry fileEntry = new FileEntry();
        final String id = entriesStream.readString();
        FileEntry previous = mapping.put(id, fileEntry);
        if (previous != null) {
          throw new CorruptIndexException("Duplicate cfs entry id=" + id + " in CFS ", entriesStream);
        }
        fileEntry.offset = entriesStream.readLong();
        fileEntry.length = entriesStream.readLong();
      }
      if (version >= Lucene40CompoundWriter.VERSION_CHECKSUM) {
        CodecUtil.checkFooter(entriesStream);
      } else {
        CodecUtil.checkEOF(entriesStream);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(entriesStream);
      } else {
        IOUtils.closeWhileHandlingException(entriesStream);
      }
    }
    return mapping;
  }
  
  public Directory getDirectory() {
    return directory;
  }
  
  public String getName() {
    return fileName;
  }
  
  @Override
  public synchronized void close() throws IOException {
    if (!isOpen) {
      // allow double close - usually to be consistent with other closeables
      return; // already closed
     }
    isOpen = false;
    if (writer != null) {
      assert openForWrite;
      writer.close();
    } else {
      IOUtils.close(handle);
    }
  }
  
  @Override
  public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    assert !openForWrite;
    final String id = IndexFileNames.stripSegmentName(name);
    final FileEntry entry = entries.get(id);
    if (entry == null) {
      throw new FileNotFoundException("No sub-file with id " + id + " found (fileName=" + name + " files: " + entries.keySet() + ")");
    }
    return handle.slice(name, entry.offset, entry.length);
  }
  
  /** Returns an array of strings, one for each file in the directory. */
  @Override
  public String[] listAll() {
    ensureOpen();
    String[] res;
    if (writer != null) {
      res = writer.listAll(); 
    } else {
      res = entries.keySet().toArray(new String[entries.size()]);
      // Add the segment name
      String seg = IndexFileNames.parseSegmentName(fileName);
      for (int i = 0; i < res.length; i++) {
        res[i] = seg + res[i];
      }
    }
    return res;
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException always: not supported by CFS */
  @Override
  public void deleteFile(String name) {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException always: not supported by CFS */
  public void renameFile(String from, String to) {
    throw new UnsupportedOperationException();
  }
  
  /** Returns the length of a file in the directory.
   * @throws IOException if the file does not exist */
  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    if (this.writer != null) {
      return writer.fileLength(name);
    }
    FileEntry e = entries.get(IndexFileNames.stripSegmentName(name));
    if (e == null)
      throw new FileNotFoundException(name);
    return e.length;
  }
  
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (!openForWrite) {
      throw new UnsupportedOperationException();
    }
    return writer.createOutput(name, context);
  }
  
  @Override
  public void sync(Collection<String> names) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Lock obtainLock(String name) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String toString() {
    return "CompoundFileDirectory(file=\"" + fileName + "\" in dir=" + directory + ")";
  }
}
