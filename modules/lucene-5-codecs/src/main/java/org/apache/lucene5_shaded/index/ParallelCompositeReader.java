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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/** An {@link CompositeReader} which reads multiple, parallel indexes.  Each
 * index added must have the same number of documents, and exactly the same
 * number of leaves (with equal {@code maxDoc}), but typically each contains
 * different fields. Deletions are taken from the first reader. Each document
 * contains the union of the fields of all documents with the same document
 * number.  When searching, matches for a query term are from the first index
 * added that has the field.
 *
 * <p>This is useful, e.g., with collections that have large fields which
 * change rarely and small fields that change more frequently.  The smaller
 * fields may be re-indexed in a new index and both indexes may be searched
 * together.
 * 
 * <p><strong>Warning:</strong> It is up to you to make sure all indexes
 * are created and modified the same way. For example, if you add
 * documents to one index, you need to add the same documents in the
 * same order to the other indexes. <em>Failure to do so will result in
 * undefined behavior</em>.
 * A good strategy to create suitable indexes with {@link IndexWriter} is to use
 * {@link LogDocMergePolicy}, as this one does not reorder documents
 * during merging (like {@code TieredMergePolicy}) and triggers merges
 * by number of documents per segment. If you use different {@link MergePolicy}s
 * it might happen that the segment structure of your index is no longer predictable.
 */
public class ParallelCompositeReader extends BaseCompositeReader<LeafReader> {
  private final boolean closeSubReaders;
  private final Set<IndexReader> completeReaderSet =
    Collections.newSetFromMap(new IdentityHashMap<IndexReader,Boolean>());

  /** Create a ParallelCompositeReader based on the provided
   *  readers; auto-closes the given readers on {@link #close()}. */
  public ParallelCompositeReader(CompositeReader... readers) throws IOException {
    this(true, readers);
  }

  /** Create a ParallelCompositeReader based on the provided
   *  readers. */
  public ParallelCompositeReader(boolean closeSubReaders, CompositeReader... readers) throws IOException {
    this(closeSubReaders, readers, readers);
  }

  /** Expert: create a ParallelCompositeReader based on the provided
   *  readers and storedFieldReaders; when a document is
   *  loaded, only storedFieldsReaders will be used. */
  public ParallelCompositeReader(boolean closeSubReaders, CompositeReader[] readers, CompositeReader[] storedFieldReaders) throws IOException {
    super(prepareLeafReaders(readers, storedFieldReaders));
    this.closeSubReaders = closeSubReaders;
    Collections.addAll(completeReaderSet, readers);
    Collections.addAll(completeReaderSet, storedFieldReaders);
    // update ref-counts (like MultiReader):
    if (!closeSubReaders) {
      for (final IndexReader reader : completeReaderSet) {
        reader.incRef();
      }
    }
    // finally add our own synthetic readers, so we close or decRef them, too (it does not matter what we do)
    completeReaderSet.addAll(getSequentialSubReaders());
  }

  private static LeafReader[] prepareLeafReaders(CompositeReader[] readers, CompositeReader[] storedFieldsReaders) throws IOException {
    if (readers.length == 0) {
      if (storedFieldsReaders.length > 0)
        throw new IllegalArgumentException("There must be at least one main reader if storedFieldsReaders are used.");
      return new LeafReader[0];
    } else {
      final List<? extends LeafReaderContext> firstLeaves = readers[0].leaves();

      // check compatibility:
      final int maxDoc = readers[0].maxDoc(), noLeaves = firstLeaves.size();
      final int[] leafMaxDoc = new int[noLeaves];
      for (int i = 0; i < noLeaves; i++) {
        final LeafReader r = firstLeaves.get(i).reader();
        leafMaxDoc[i] = r.maxDoc();
      }
      validate(readers, maxDoc, leafMaxDoc);
      validate(storedFieldsReaders, maxDoc, leafMaxDoc);

      // flatten structure of each Composite to just LeafReader[]
      // and combine parallel structure with ParallelLeafReaders:
      final LeafReader[] wrappedLeaves = new LeafReader[noLeaves];
      for (int i = 0; i < wrappedLeaves.length; i++) {
        final LeafReader[] subs = new LeafReader[readers.length];
        for (int j = 0; j < readers.length; j++) {
          subs[j] = readers[j].leaves().get(i).reader();
        }
        final LeafReader[] storedSubs = new LeafReader[storedFieldsReaders.length];
        for (int j = 0; j < storedFieldsReaders.length; j++) {
          storedSubs[j] = storedFieldsReaders[j].leaves().get(i).reader();
        }
        // We pass true for closeSubs and we prevent touching of subreaders in doClose():
        // By this the synthetic throw-away readers used here are completely invisible to ref-counting
        wrappedLeaves[i] = new ParallelLeafReader(true, subs, storedSubs) {
          @Override
          protected void doClose() {}
        };
      }
      return wrappedLeaves;
    }
  }
  
  private static void validate(CompositeReader[] readers, int maxDoc, int[] leafMaxDoc) {
    for (int i = 0; i < readers.length; i++) {
      final CompositeReader reader = readers[i];
      final List<? extends LeafReaderContext> subs = reader.leaves();
      if (reader.maxDoc() != maxDoc) {
        throw new IllegalArgumentException("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
      }
      final int noSubs = subs.size();
      if (noSubs != leafMaxDoc.length) {
        throw new IllegalArgumentException("All readers must have same number of leaf readers");
      }
      for (int subIDX = 0; subIDX < noSubs; subIDX++) {
        final LeafReader r = subs.get(subIDX).reader();
        if (r.maxDoc() != leafMaxDoc[subIDX]) {
          throw new IllegalArgumentException("All leaf readers must have same corresponding subReader maxDoc");
        }
      }
    }    
  }
  
  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (final IndexReader reader : completeReaderSet) {
      try {
        if (closeSubReaders) {
          reader.close();
        } else {
          reader.decRef();
        }
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }
    // throw the first exception
    if (ioe != null) throw ioe;
  }
}
