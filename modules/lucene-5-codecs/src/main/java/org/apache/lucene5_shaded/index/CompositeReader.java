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


import java.util.List;

import org.apache.lucene5_shaded.store.*;

/**
 Instances of this reader type can only
 be used to get stored fields from the underlying LeafReaders,
 but it is not possible to directly retrieve postings. To do that, get
 the {@link LeafReaderContext} for all sub-readers via {@link #leaves()}.
 Alternatively, you can mimic an {@link LeafReader} (with a serious slowdown),
 by wrapping composite readers with {@link SlowCompositeReaderWrapper}.
 
 <p>IndexReader instances for indexes on disk are usually constructed
 with a call to one of the static <code>DirectoryReader.open()</code> methods,
 e.g. {@link DirectoryReader#open(Directory)}. {@link DirectoryReader} implements
 the {@code CompositeReader} interface, it is not possible to directly get postings.
 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 one of the static <code>open()</code> methods, e.g. {@link
 DirectoryReader#open(Directory)}.

 <p> For efficiency, in this API documents are often referred to via
 <i>document numbers</i>, non-negative integers which each name a unique
 document in the index.  These document numbers are ephemeral -- they may change
 as documents are added to and deleted from an index.  Clients should thus not
 rely on a given document having the same number between sessions.

 <p>
 <a name="thread-safety"></a><p><b>NOTE</b>: {@link
 IndexReader} instances are completely thread
 safe, meaning multiple threads can call any of its methods,
 concurrently.  If your application requires external
 synchronization, you should <b>not</b> synchronize on the
 <code>IndexReader</code> instance; use your own
 (non-Lucene) objects instead.
*/
public abstract class CompositeReader extends IndexReader {

  private volatile CompositeReaderContext readerContext = null; // lazy init

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected CompositeReader() { 
    super();
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    // walk up through class hierarchy to get a non-empty simple name (anonymous classes have no name):
    for (Class<?> clazz = getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      if (!clazz.isAnonymousClass()) {
        buffer.append(clazz.getSimpleName());
        break;
      }
    }
    buffer.append('(');
    final List<? extends IndexReader> subReaders = getSequentialSubReaders();
    assert subReaders != null;
    if (!subReaders.isEmpty()) {
      buffer.append(subReaders.get(0));
      for (int i = 1, c = subReaders.size(); i < c; ++i) {
        buffer.append(" ").append(subReaders.get(i));
      }
    }
    buffer.append(')');
    return buffer.toString();
  }
  
  /** Expert: returns the sequential sub readers that this
   *  reader is logically composed of. This method may not
   *  return {@code null}.
   *  
   *  <p><b>NOTE:</b> In contrast to previous Lucene versions this method
   *  is no longer public, code that wants to get all {@link LeafReader}s
   *  this composite is composed of should use {@link IndexReader#leaves()}.
   * @see IndexReader#leaves()
   */
  protected abstract List<? extends IndexReader> getSequentialSubReaders();

  @Override
  public final CompositeReaderContext getContext() {
    ensureOpen();
    // lazy init without thread safety for perf reasons: Building the readerContext twice does not hurt!
    if (readerContext == null) {
      assert getSequentialSubReaders() != null;
      readerContext = CompositeReaderContext.create(this);
    }
    return readerContext;
  }
}
