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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene5_shaded.util.Bits;

/** An {@link LeafReader} which reads multiple, parallel indexes.  Each index
 * added must have the same number of documents, but typically each contains
 * different fields. Deletions are taken from the first reader.
 * Each document contains the union of the fields of all documents
 * with the same document number.  When searching, matches for a
 * query term are from the first index added that has the field.
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
 */
public class ParallelLeafReader extends LeafReader {
  private final FieldInfos fieldInfos;
  private final ParallelFields fields = new ParallelFields();
  private final LeafReader[] parallelReaders, storedFieldsReaders;
  private final Set<LeafReader> completeReaderSet =
    Collections.newSetFromMap(new IdentityHashMap<LeafReader,Boolean>());
  private final boolean closeSubReaders;
  private final int maxDoc, numDocs;
  private final boolean hasDeletions;
  private final SortedMap<String,LeafReader> fieldToReader = new TreeMap<>();
  private final SortedMap<String,LeafReader> tvFieldToReader = new TreeMap<>();
  
  /** Create a ParallelLeafReader based on the provided
   *  readers; auto-closes the given readers on {@link #close()}. */
  public ParallelLeafReader(LeafReader... readers) throws IOException {
    this(true, readers);
  }

  /** Create a ParallelLeafReader based on the provided
   *  readers. */
  public ParallelLeafReader(boolean closeSubReaders, LeafReader... readers) throws IOException {
    this(closeSubReaders, readers, readers);
  }

  /** Expert: create a ParallelLeafReader based on the provided
   *  readers and storedFieldReaders; when a document is
   *  loaded, only storedFieldsReaders will be used. */
  public ParallelLeafReader(boolean closeSubReaders, LeafReader[] readers, LeafReader[] storedFieldsReaders) throws IOException {
    this.closeSubReaders = closeSubReaders;
    if (readers.length == 0 && storedFieldsReaders.length > 0)
      throw new IllegalArgumentException("There must be at least one main reader if storedFieldsReaders are used.");
    this.parallelReaders = readers.clone();
    this.storedFieldsReaders = storedFieldsReaders.clone();
    if (parallelReaders.length > 0) {
      final LeafReader first = parallelReaders[0];
      this.maxDoc = first.maxDoc();
      this.numDocs = first.numDocs();
      this.hasDeletions = first.hasDeletions();
    } else {
      this.maxDoc = this.numDocs = 0;
      this.hasDeletions = false;
    }
    Collections.addAll(completeReaderSet, this.parallelReaders);
    Collections.addAll(completeReaderSet, this.storedFieldsReaders);
    
    // check compatibility:
    for(LeafReader reader : completeReaderSet) {
      if (reader.maxDoc() != maxDoc) {
        throw new IllegalArgumentException("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
      }
    }
    
    // TODO: make this read-only in a cleaner way?
    FieldInfos.Builder builder = new FieldInfos.Builder();
    // build FieldInfos and fieldToReader map:
    for (final LeafReader reader : this.parallelReaders) {
      final FieldInfos readerFieldInfos = reader.getFieldInfos();
      for (FieldInfo fieldInfo : readerFieldInfos) {
        // NOTE: first reader having a given field "wins":
        if (!fieldToReader.containsKey(fieldInfo.name)) {
          builder.add(fieldInfo);
          fieldToReader.put(fieldInfo.name, reader);
          if (fieldInfo.hasVectors()) {
            tvFieldToReader.put(fieldInfo.name, reader);
          }
        }
      }
    }
    fieldInfos = builder.finish();
    
    // build Fields instance
    for (final LeafReader reader : this.parallelReaders) {
      final Fields readerFields = reader.fields();
      for (String field : readerFields) {
        // only add if the reader responsible for that field name is the current:
        if (fieldToReader.get(field) == reader) {
          this.fields.addField(field, readerFields.terms(field));
        }
      }
    }

    // do this finally so any Exceptions occurred before don't affect refcounts:
    for (LeafReader reader : completeReaderSet) {
      if (!closeSubReaders) {
        reader.incRef();
      }
      reader.registerParentReader(this);
    }
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("ParallelLeafReader(");
    for (final Iterator<LeafReader> iter = completeReaderSet.iterator(); iter.hasNext();) {
      buffer.append(iter.next());
      if (iter.hasNext()) buffer.append(", ");
    }
    return buffer.append(')').toString();
  }

  @Override
  public void addCoreClosedListener(CoreClosedListener listener) {
    addCoreClosedListenerAsReaderClosedListener(this, listener);
  }

  @Override
  public void removeCoreClosedListener(CoreClosedListener listener) {
    removeCoreClosedListenerAsReaderClosedListener(this, listener);
  }

  // Single instance of this, per ParallelReader instance
  private final class ParallelFields extends Fields {
    final Map<String,Terms> fields = new TreeMap<>();
    
    ParallelFields() {
    }
    
    void addField(String fieldName, Terms terms) {
      fields.put(fieldName, terms);
    }
    
    @Override
    public Iterator<String> iterator() {
      return Collections.unmodifiableSet(fields.keySet()).iterator();
    }
    
    @Override
    public Terms terms(String field) {
      return fields.get(field);
    }
    
    @Override
    public int size() {
      return fields.size();
    }
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * NOTE: the returned field numbers will likely not
   * correspond to the actual field numbers in the underlying
   * readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   * will be unavailable.
   */
  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }
  
  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return hasDeletions ? parallelReaders[0].getLiveDocs() : null;
  }
  
  @Override
  public Fields fields() {
    ensureOpen();
    return fields;
  }
  
  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }
  
  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }
  
  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    for (final LeafReader reader: storedFieldsReaders) {
      reader.document(docID, visitor);
    }
  }
  
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    ParallelFields fields = null;
    for (Map.Entry<String,LeafReader> ent : tvFieldToReader.entrySet()) {
      String fieldName = ent.getKey();
      Terms vector = ent.getValue().getTermVector(docID, fieldName);
      if (vector != null) {
        if (fields == null) {
          fields = new ParallelFields();
        }
        fields.addField(fieldName, vector);
      }
    }
    
    return fields;
  }
  
  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (LeafReader reader : completeReaderSet) {
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

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    LeafReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getNumericDocValues(field);
  }
  
  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    LeafReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getBinaryDocValues(field);
  }
  
  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    LeafReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getSortedDocValues(field);
  }
  
  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    LeafReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getSortedNumericDocValues(field);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    LeafReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getSortedSetDocValues(field);
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    ensureOpen();
    LeafReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getDocsWithField(field);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    LeafReader reader = fieldToReader.get(field);
    NumericDocValues values = reader == null ? null : reader.getNormValues(field);
    return values;
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    for (LeafReader reader : completeReaderSet) {
      reader.checkIntegrity();
    }
  }

  /** Returns the {@link LeafReader}s that were passed on init. */
  public LeafReader[] getParallelReaders() {
    ensureOpen();
    return parallelReaders;
  }
}
