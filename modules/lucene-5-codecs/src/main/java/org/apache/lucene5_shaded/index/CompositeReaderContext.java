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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link IndexReaderContext} for {@link CompositeReader} instance.
 */
public final class CompositeReaderContext extends IndexReaderContext {
  private final List<IndexReaderContext> children;
  private final List<LeafReaderContext> leaves;
  private final CompositeReader reader;
  
  static CompositeReaderContext create(CompositeReader reader) {
    return new Builder(reader).build();
  }

  /**
   * Creates a {@link CompositeReaderContext} for intermediate readers that aren't
   * not top-level readers in the current context
   */
  CompositeReaderContext(CompositeReaderContext parent, CompositeReader reader,
      int ordInParent, int docbaseInParent, List<IndexReaderContext> children) {
    this(parent, reader, ordInParent, docbaseInParent, children, null);
  }
  
  /**
   * Creates a {@link CompositeReaderContext} for top-level readers with parent set to <code>null</code>
   */
  CompositeReaderContext(CompositeReader reader, List<IndexReaderContext> children, List<LeafReaderContext> leaves) {
    this(null, reader, 0, 0, children, leaves);
  }
  
  private CompositeReaderContext(CompositeReaderContext parent, CompositeReader reader,
      int ordInParent, int docbaseInParent, List<IndexReaderContext> children,
      List<LeafReaderContext> leaves) {
    super(parent, ordInParent, docbaseInParent);
    this.children = Collections.unmodifiableList(children);
    this.leaves = leaves == null ? null : Collections.unmodifiableList(leaves);
    this.reader = reader;
  }

  @Override
  public List<LeafReaderContext> leaves() throws UnsupportedOperationException {
    if (!isTopLevel)
      throw new UnsupportedOperationException("This is not a top-level context.");
    assert leaves != null;
    return leaves;
  }
  
  
  @Override
  public List<IndexReaderContext> children() {
    return children;
  }
  
  @Override
  public CompositeReader reader() {
    return reader;
  }
  
  private static final class Builder {
    private final CompositeReader reader;
    private final List<LeafReaderContext> leaves = new ArrayList<>();
    private int leafDocBase = 0;
    
    public Builder(CompositeReader reader) {
      this.reader = reader;
    }
    
    public CompositeReaderContext build() {
      return (CompositeReaderContext) build(null, reader, 0, 0);
    }
    
    private IndexReaderContext build(CompositeReaderContext parent, IndexReader reader, int ord, int docBase) {
      if (reader instanceof LeafReader) {
        final LeafReader ar = (LeafReader) reader;
        final LeafReaderContext atomic = new LeafReaderContext(parent, ar, ord, docBase, leaves.size(), leafDocBase);
        leaves.add(atomic);
        leafDocBase += reader.maxDoc();
        return atomic;
      } else {
        final CompositeReader cr = (CompositeReader) reader;
        final List<? extends IndexReader> sequentialSubReaders = cr.getSequentialSubReaders();
        final List<IndexReaderContext> children = Arrays.asList(new IndexReaderContext[sequentialSubReaders.size()]);
        final CompositeReaderContext newParent;
        if (parent == null) {
          newParent = new CompositeReaderContext(cr, children, leaves);
        } else {
          newParent = new CompositeReaderContext(parent, cr, ord, docBase, children);
        }
        int newDocBase = 0;
        for (int i = 0, c = sequentialSubReaders.size(); i < c; i++) {
          final IndexReader r = sequentialSubReaders.get(i);
          children.set(i, build(newParent, r, i, newDocBase));
          newDocBase += r.maxDoc();
        }
        assert newDocBase == cr.maxDoc();
        return newParent;
      }
    }
  }

}