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
import java.util.List;

/**
 * A FilterDirectoryReader wraps another DirectoryReader, allowing implementations
 * to transform or extend it.
 *
 * Subclasses should implement doWrapDirectoryReader to return an instance of the
 * subclass.
 *
 * If the subclass wants to wrap the DirectoryReader's subreaders, it should also
 * implement a SubReaderWrapper subclass, and pass an instance to its super
 * constructor.
 */
public abstract class FilterDirectoryReader extends DirectoryReader {

  /** Get the wrapped instance by <code>reader</code> as long as this reader is
   *  an instance of {@link FilterDirectoryReader}.  */
  public static DirectoryReader unwrap(DirectoryReader reader) {
    while (reader instanceof FilterDirectoryReader) {
      reader = ((FilterDirectoryReader) reader).in;
    }
    return reader;
  }

  /**
   * Factory class passed to FilterDirectoryReader constructor that allows
   * subclasses to wrap the filtered DirectoryReader's subreaders.  You
   * can use this to, e.g., wrap the subreaders with specialised
   * FilterLeafReader implementations.
   */
  public static abstract class SubReaderWrapper {

    private LeafReader[] wrap(List<? extends LeafReader> readers) {
      LeafReader[] wrapped = new LeafReader[readers.size()];
      for (int i = 0; i < readers.size(); i++) {
        wrapped[i] = wrap(readers.get(i));
      }
      return wrapped;
    }

    /** Constructor */
    public SubReaderWrapper() {}

    /**
     * Wrap one of the parent DirectoryReader's subreaders
     * @param reader the subreader to wrap
     * @return a wrapped/filtered LeafReader
     */
    public abstract LeafReader wrap(LeafReader reader);

  }

  /** The filtered DirectoryReader */
  protected final DirectoryReader in;

  /**
   * Create a new FilterDirectoryReader that filters a passed in DirectoryReader,
   * using the supplied SubReaderWrapper to wrap its subreader.
   * @param in the DirectoryReader to filter
   * @param wrapper the SubReaderWrapper to use to wrap subreaders
   */
  public FilterDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper) throws IOException {
    super(in.directory(), wrapper.wrap(in.getSequentialSubReaders()));
    this.in = in;
  }

  /**
   * Called by the doOpenIfChanged() methods to return a new wrapped DirectoryReader.
   *
   * Implementations should just return an instantiation of themselves, wrapping the
   * passed in DirectoryReader.
   *
   * @param in the DirectoryReader to wrap
   * @return the wrapped DirectoryReader
   */
  protected abstract DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException;

  private final DirectoryReader wrapDirectoryReader(DirectoryReader in) throws IOException {
    return in == null ? null : doWrapDirectoryReader(in);
  }

  @Override
  protected final DirectoryReader doOpenIfChanged() throws IOException {
    return wrapDirectoryReader(in.doOpenIfChanged());
  }

  @Override
  protected final DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
    return wrapDirectoryReader(in.doOpenIfChanged(commit));
  }

  @Override
  protected final DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
    return wrapDirectoryReader(in.doOpenIfChanged(writer, applyAllDeletes));
  }

  @Override
  public long getVersion() {
    return in.getVersion();
  }

  @Override
  public boolean isCurrent() throws IOException {
    return in.isCurrent();
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    return in.getIndexCommit();
  }

  @Override
  protected void doClose() throws IOException {
    in.close();
  }

  /** Returns the wrapped {@link DirectoryReader}. */
  public DirectoryReader getDelegate() {
    return in;
  }
}
