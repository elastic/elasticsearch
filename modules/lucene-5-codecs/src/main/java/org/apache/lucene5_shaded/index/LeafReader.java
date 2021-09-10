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


import org.apache.lucene5_shaded.util.Bits;

import java.io.IOException;

/** {@code LeafReader} is an abstract class, providing an interface for accessing an
 index.  Search of an index is done entirely through this abstract interface,
 so that any subclass which implements it is searchable. IndexReaders implemented
 by this subclass do not consist of several sub-readers,
 they are atomic. They support retrieval of stored fields, doc values, terms,
 and postings.

 <p>For efficiency, in this API documents are often referred to via
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
public abstract class LeafReader extends IndexReader {

  private final LeafReaderContext readerContext = new LeafReaderContext(this);

  /** Sole constructor. (For invocation by subclass
   *  constructors, typically implicit.) */
  protected LeafReader() {
    super();
  }

  @Override
  public final LeafReaderContext getContext() {
    ensureOpen();
    return readerContext;
  }

  /**
   * Called when the shared core for this {@link LeafReader}
   * is closed.
   * <p>
   * If this {@link LeafReader} impl has the ability to share
   * resources across instances that might only vary through
   * deleted documents and doc values updates, then this listener
   * will only be called when the shared core is closed.
   * Otherwise, this listener will be called when this reader is
   * closed.</p>
   * <p>
   * This is typically useful to manage per-segment caches: when
   * the listener is called, it is safe to evict this reader from
   * any caches keyed on {@link #getCoreCacheKey}.</p>
   *
   * @lucene.experimental
   */
  public static interface CoreClosedListener {
    /** Invoked when the shared core of the original {@code
     *  SegmentReader} has closed. The provided {@code
     *  ownerCoreCacheKey} will be the same key as the one
     *  returned by {@link LeafReader#getCoreCacheKey()}. */
    public void onClose(Object ownerCoreCacheKey) throws IOException;
  }

  private static class CoreClosedListenerWrapper implements ReaderClosedListener {

    private final CoreClosedListener listener;

    CoreClosedListenerWrapper(CoreClosedListener listener) {
      this.listener = listener;
    }

    @Override
    public void onClose(IndexReader reader) throws IOException {
      listener.onClose(reader.getCoreCacheKey());
    }

    @Override
    public int hashCode() {
      return listener.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof CoreClosedListenerWrapper)) {
        return false;
      }
      return listener.equals(((CoreClosedListenerWrapper) other).listener);
    }

  }

  /** Add a {@link CoreClosedListener} as a {@link ReaderClosedListener}. This
   * method is typically useful for {@link LeafReader} implementations that
   * don't have the concept of a core that is shared across several
   * {@link LeafReader} instances in which case the {@link CoreClosedListener}
   * is called when closing the reader. */
  protected static void addCoreClosedListenerAsReaderClosedListener(IndexReader reader, CoreClosedListener listener) {
    reader.addReaderClosedListener(new CoreClosedListenerWrapper(listener));
  }

  /** Remove a {@link CoreClosedListener} which has been added with
   * {@link #addCoreClosedListenerAsReaderClosedListener(IndexReader, CoreClosedListener)}. */
  protected static void removeCoreClosedListenerAsReaderClosedListener(IndexReader reader, CoreClosedListener listener) {
    reader.removeReaderClosedListener(new CoreClosedListenerWrapper(listener));
  }

  /** Expert: adds a CoreClosedListener to this reader's shared core
   *  @lucene.experimental */
  public abstract void addCoreClosedListener(CoreClosedListener listener);

  /** Expert: removes a CoreClosedListener from this reader's shared core
   *  @lucene.experimental */
  public abstract void removeCoreClosedListener(CoreClosedListener listener);

  /**
   * Returns {@link Fields} for this reader.
   * This method will not return null.
   */
  public abstract Fields fields() throws IOException;

  @Override
  public final int docFreq(Term term) throws IOException {
    final Terms terms = terms(term.field());
    if (terms == null) {
      return 0;
    }
    final TermsEnum termsEnum = terms.iterator();
    if (termsEnum.seekExact(term.bytes())) {
      return termsEnum.docFreq();
    } else {
      return 0;
    }
  }

  /** Returns the number of documents containing the term
   * <code>t</code>.  This method returns 0 if the term or
   * field does not exists.  This method does not take into
   * account deleted documents that have not yet been merged
   * away. */
  @Override
  public final long totalTermFreq(Term term) throws IOException {
    final Terms terms = terms(term.field());
    if (terms == null) {
      return 0;
    }
    final TermsEnum termsEnum = terms.iterator();
    if (termsEnum.seekExact(term.bytes())) {
      return termsEnum.totalTermFreq();
    } else {
      return 0;
    }
  }

  @Override
  public final long getSumDocFreq(String field) throws IOException {
    final Terms terms = terms(field);
    if (terms == null) {
      return 0;
    }
    return terms.getSumDocFreq();
  }

  @Override
  public final int getDocCount(String field) throws IOException {
    final Terms terms = terms(field);
    if (terms == null) {
      return 0;
    }
    return terms.getDocCount();
  }

  @Override
  public final long getSumTotalTermFreq(String field) throws IOException {
    final Terms terms = terms(field);
    if (terms == null) {
      return 0;
    }
    return terms.getSumTotalTermFreq();
  }

  /** This may return null if the field does not exist.*/
  public final Terms terms(String field) throws IOException {
    return fields().terms(field);
  }

  /** Returns {@link PostingsEnum} for the specified term.
   *  This will return null if either the field or
   *  term does not exist.
   *  <p><b>NOTE:</b> The returned {@link PostingsEnum} may contain deleted docs.
   *  @see TermsEnum#postings(PostingsEnum) */
  public final PostingsEnum postings(Term term, int flags) throws IOException {
    assert term.field() != null;
    assert term.bytes() != null;
    final Terms terms = terms(term.field());
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      if (termsEnum.seekExact(term.bytes())) {
        return termsEnum.postings(null, flags);
      }
    }
    return null;
  }

  /** Returns {@link PostingsEnum} for the specified term
   *  with {@link PostingsEnum#FREQS}.
   *  <p>
   *  Use this method if you only require documents and frequencies,
   *  and do not need any proximity data.
   *  This method is equivalent to 
   *  {@link #postings(Term, int) postings(term, PostingsEnum.FREQS)}
   *  <p><b>NOTE:</b> The returned {@link PostingsEnum} may contain deleted docs.
   *  @see #postings(Term, int)
   */
  public final PostingsEnum postings(Term term) throws IOException {
    return postings(term, PostingsEnum.FREQS);
  }

  /** Returns {@link NumericDocValues} for this field, or
   *  null if no {@link NumericDocValues} were indexed for
   *  this field.  The returned instance should only be
   *  used by a single thread. */
  public abstract NumericDocValues getNumericDocValues(String field) throws IOException;

  /** Returns {@link BinaryDocValues} for this field, or
   *  null if no {@link BinaryDocValues} were indexed for
   *  this field.  The returned instance should only be
   *  used by a single thread. */
  public abstract BinaryDocValues getBinaryDocValues(String field) throws IOException;

  /** Returns {@link SortedDocValues} for this field, or
   *  null if no {@link SortedDocValues} were indexed for
   *  this field.  The returned instance should only be
   *  used by a single thread. */
  public abstract SortedDocValues getSortedDocValues(String field) throws IOException;
  
  /** Returns {@link SortedNumericDocValues} for this field, or
   *  null if no {@link SortedNumericDocValues} were indexed for
   *  this field.  The returned instance should only be
   *  used by a single thread. */
  public abstract SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException;

  /** Returns {@link SortedSetDocValues} for this field, or
   *  null if no {@link SortedSetDocValues} were indexed for
   *  this field.  The returned instance should only be
   *  used by a single thread. */
  public abstract SortedSetDocValues getSortedSetDocValues(String field) throws IOException;

  /** Returns a {@link Bits} at the size of <code>reader.maxDoc()</code>,
   *  with turned on bits for each docid that does have a value for this field,
   *  or null if no DocValues were indexed for this field. The
   *  returned instance should only be used by a single thread */
  public abstract Bits getDocsWithField(String field) throws IOException;

  /** Returns {@link NumericDocValues} representing norms
   *  for this field, or null if no {@link NumericDocValues}
   *  were indexed. The returned instance should only be
   *  used by a single thread. */
  public abstract NumericDocValues getNormValues(String field) throws IOException;

  /**
   * Get the {@link FieldInfos} describing all fields in
   * this reader.
   * @lucene.experimental
   */
  public abstract FieldInfos getFieldInfos();

  /** Returns the {@link Bits} representing live (not
   *  deleted) docs.  A set bit indicates the doc ID has not
   *  been deleted.  If this method returns null it means
   *  there are no deleted documents (all documents are
   *  live).
   *
   *  The returned instance has been safely published for
   *  use by multiple threads without additional
   *  synchronization.
   */
  public abstract Bits getLiveDocs();

  /**
   * Checks consistency of this reader.
   * <p>
   * Note that this may be costly in terms of I/O, e.g.
   * may involve computing a checksum value against large data files.
   * @lucene.internal
   */
  public abstract void checkIntegrity() throws IOException;
  
  /** Returns {@link DocsEnum} for the specified term.
   *  This will return null if either the field or
   *  term does not exist.
   *  @deprecated use {@link #postings(Term)} instead */
  @Deprecated
  public final DocsEnum termDocsEnum(Term term) throws IOException {
    assert term.field() != null;
    assert term.bytes() != null;
    final Terms terms = terms(term.field());
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      if (termsEnum.seekExact(term.bytes())) {
        return termsEnum.docs(getLiveDocs(), null);
      }
    }
    return null;
  }

  /** Returns {@link DocsAndPositionsEnum} for the specified
   *  term.  This will return null if the
   *  field or term does not exist or positions weren't indexed.
   *  @deprecated use {@link #postings(Term, int)} instead */
  @Deprecated
  public final DocsAndPositionsEnum termPositionsEnum(Term term) throws IOException {
    assert term.field() != null;
    assert term.bytes() != null;
    final Terms terms = terms(term.field());
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      if (termsEnum.seekExact(term.bytes())) {
        return termsEnum.docsAndPositions(getLiveDocs(), null);
      }
    }
    return null;
  }
}
