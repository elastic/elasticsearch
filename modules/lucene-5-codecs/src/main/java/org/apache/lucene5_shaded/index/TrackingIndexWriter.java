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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene5_shaded.search.ControlledRealTimeReopenThread; // javadocs
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.store.Directory;

/** Class that tracks changes to a delegated
 *  IndexWriter, used by {@link
 *  ControlledRealTimeReopenThread} to ensure specific
 *  changes are visible.   Create this class (passing your
 *  IndexWriter), and then pass this class to {@link
 *  ControlledRealTimeReopenThread}.
 *  Be sure to make all changes via the
 *  TrackingIndexWriter, otherwise {@link
 *  ControlledRealTimeReopenThread} won't know about the changes.
 *
 * @lucene.experimental */

public class TrackingIndexWriter {
  private final IndexWriter writer;
  private final AtomicLong indexingGen = new AtomicLong(1);

  /** Create a {@code TrackingIndexWriter} wrapping the
   *  provided {@link IndexWriter}. */
  public TrackingIndexWriter(IndexWriter writer) {
    this.writer = writer;
  }

  /** Calls {@link
   *  IndexWriter#updateDocument(Term,Iterable)} and
   *  returns the generation that reflects this change. */
  public long updateDocument(Term t, Iterable<? extends IndexableField> d) throws IOException {
    writer.updateDocument(t, d);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link
   *  IndexWriter#updateDocuments(Term,Iterable)} and returns
   *  the generation that reflects this change. */
  public long updateDocuments(Term t, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    writer.updateDocuments(t, docs);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#deleteDocuments(Term...)} and
   *  returns the generation that reflects this change. */
  public long deleteDocuments(Term t) throws IOException {
    writer.deleteDocuments(t);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#deleteDocuments(Term...)} and
   *  returns the generation that reflects this change. */
  public long deleteDocuments(Term... terms) throws IOException {
    writer.deleteDocuments(terms);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#deleteDocuments(Query...)} and
   *  returns the generation that reflects this change. */
  public long deleteDocuments(Query q) throws IOException {
    writer.deleteDocuments(q);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#deleteDocuments(Query...)}
   *  and returns the generation that reflects this change. */
  public long deleteDocuments(Query... queries) throws IOException {
    writer.deleteDocuments(queries);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#deleteAll} and returns the
   *  generation that reflects this change. */
  public long deleteAll() throws IOException {
    writer.deleteAll();
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#addDocument(Iterable)}
   *  and returns the generation that reflects this change. */
  public long addDocument(Iterable<? extends IndexableField> d) throws IOException {
    writer.addDocument(d);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#addDocuments(Iterable)} and
   *  returns the generation that reflects this change. */
  public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    writer.addDocuments(docs);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#addIndexes(Directory...)} and
   *  returns the generation that reflects this change. */
  public long addIndexes(Directory... dirs) throws IOException {
    writer.addIndexes(dirs);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Calls {@link IndexWriter#addIndexes(CodecReader...)}
   *  and returns the generation that reflects this change. */
  public long addIndexes(CodecReader... readers) throws IOException {
    writer.addIndexes(readers);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  /** Return the current generation being indexed. */
  public long getGeneration() {
    return indexingGen.get();
  }

  /** Return the wrapped {@link IndexWriter}. */
  public IndexWriter getIndexWriter() {
    return writer;
  }

  /** Return and increment current gen.
   *
   * @lucene.internal */
  public long getAndIncrementGeneration() {
    return indexingGen.getAndIncrement();
  }

  /** Cals {@link
   *  IndexWriter#tryDeleteDocument(IndexReader,int)} and
   *  returns the generation that reflects this change. */
  public long tryDeleteDocument(IndexReader reader, int docID) throws IOException {
    if (writer.tryDeleteDocument(reader, docID)) {
      return indexingGen.get();
    } else {
      return -1;
    }
  }
}

