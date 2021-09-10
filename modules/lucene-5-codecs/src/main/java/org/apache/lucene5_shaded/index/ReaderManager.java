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

import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.search.ReferenceManager;
import org.apache.lucene5_shaded.search.SearcherManager;
import org.apache.lucene5_shaded.store.Directory;

/**
 * Utility class to safely share {@link DirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 * 
 * @see SearcherManager
 * 
 * @lucene.experimental
 */
public final class ReaderManager extends ReferenceManager<DirectoryReader> {

  /**
   * Creates and returns a new ReaderManager from the given
   * {@link IndexWriter}.
   * 
   * @param writer
   *          the IndexWriter to open the IndexReader from.
   * 
   * @throws IOException If there is a low-level I/O error
   */
  public ReaderManager(IndexWriter writer) throws IOException {
    this(writer, true);
  }

  /**
   * Expert: creates and returns a new ReaderManager from the given
   * {@link IndexWriter}, controlling whether past deletions should be applied.
   * 
   * @param writer
   *          the IndexWriter to open the IndexReader from.
   * @param applyAllDeletes
   *          If <code>true</code>, all buffered deletes will be applied (made
   *          visible) in the {@link IndexSearcher} / {@link DirectoryReader}.
   *          If <code>false</code>, the deletes may or may not be applied, but
   *          remain buffered (in IndexWriter) so that they will be applied in
   *          the future. Applying deletes can be costly, so if your app can
   *          tolerate deleted documents being returned you might gain some
   *          performance by passing <code>false</code>. See
   *          {@link DirectoryReader#openIfChanged(DirectoryReader, IndexWriter, boolean)}.
   * 
   * @throws IOException If there is a low-level I/O error
   */
  public ReaderManager(IndexWriter writer, boolean applyAllDeletes) throws IOException {
    current = DirectoryReader.open(writer, applyAllDeletes);
  }
  
  /**
   * Creates and returns a new ReaderManager from the given {@link Directory}. 
   * @param dir the directory to open the DirectoryReader on.
   *        
   * @throws IOException If there is a low-level I/O error
   */
  public ReaderManager(Directory dir) throws IOException {
    current = DirectoryReader.open(dir);
  }

  /**
   * Creates and returns a new ReaderManager from the given
   * already-opened {@link DirectoryReader}, stealing
   * the incoming reference.
   *
   * @param reader the directoryReader to use for future reopens
   *        
   * @throws IOException If there is a low-level I/O error
   */
  public ReaderManager(DirectoryReader reader) throws IOException {
    current = reader;
  }

  @Override
  protected void decRef(DirectoryReader reference) throws IOException {
    reference.decRef();
  }
  
  @Override
  protected DirectoryReader refreshIfNeeded(DirectoryReader referenceToRefresh) throws IOException {
    return DirectoryReader.openIfChanged(referenceToRefresh);
  }
  
  @Override
  protected boolean tryIncRef(DirectoryReader reference) {
    return reference.tryIncRef();
  }

  @Override
  protected int getRefCount(DirectoryReader reference) {
    return reference.getRefCount();
  }

}
