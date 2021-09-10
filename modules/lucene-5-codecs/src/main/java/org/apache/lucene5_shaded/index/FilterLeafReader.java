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
import java.util.Iterator;
import java.util.Objects;

import org.apache.lucene5_shaded.search.CachingWrapperQuery;
import org.apache.lucene5_shaded.util.AttributeSource;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;

/**  A <code>FilterLeafReader</code> contains another LeafReader, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * <code>FilterLeafReader</code> itself simply implements all abstract methods
 * of <code>IndexReader</code> with versions that pass all requests to the
 * contained index reader. Subclasses of <code>FilterLeafReader</code> may
 * further override some of these methods and may also provide additional
 * methods and fields.
 * <p><b>NOTE</b>: If you override {@link #getLiveDocs()}, you will likely need
 * to override {@link #numDocs()} as well and vice-versa.
 * <p><b>NOTE</b>: If this {@link FilterLeafReader} does not change the
 * content the contained reader, you could consider overriding
 * {@link #getCoreCacheKey()} so that
 * {@link CachingWrapperQuery} shares the same entries for this atomic reader
 * and the wrapped one. {@link #getCombinedCoreAndDeletesKey()} could be
 * overridden as well if the {@link #getLiveDocs() live docs} are not changed
 * either.
 */
public class FilterLeafReader extends LeafReader {

  /** Get the wrapped instance by <code>reader</code> as long as this reader is
   *  an instance of {@link FilterLeafReader}.  */
  public static LeafReader unwrap(LeafReader reader) {
    while (reader instanceof FilterLeafReader) {
      reader = ((FilterLeafReader) reader).in;
    }
    return reader;
  }

  /** Base class for filtering {@link Fields}
   *  implementations. */
  public static class FilterFields extends Fields {
    /** The underlying Fields instance. */
    protected final Fields in;

    /**
     * Creates a new FilterFields.
     * @param in the underlying Fields instance.
     */
    public FilterFields(Fields in) {
      if (in == null) {
        throw new NullPointerException("incoming Fields cannot be null");
      }
      this.in = in;
    }

    @Override
    public Iterator<String> iterator() {
      return in.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return in.terms(field);
    }

    @Override
    public int size() {
      return in.size();
    }
  }

  /** Base class for filtering {@link Terms} implementations.
   * <p><b>NOTE</b>: If the order of terms and documents is not changed, and if
   * these terms are going to be intersected with automata, you could consider
   * overriding {@link #intersect} for better performance.
   */
  public static class FilterTerms extends Terms {
    /** The underlying Terms instance. */
    protected final Terms in;

    /**
     * Creates a new FilterTerms
     * @param in the underlying Terms instance.
     */
    public FilterTerms(Terms in) {
      if (in == null) {
        throw new NullPointerException("incoming Terms cannot be null");
      }
      this.in = in;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return in.iterator();
    }

    @Override
    public long size() throws IOException {
      return in.size();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return in.getSumTotalTermFreq();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return in.getSumDocFreq();
    }

    @Override
    public int getDocCount() throws IOException {
      return in.getDocCount();
    }

    @Override
    public boolean hasFreqs() {
      return in.hasFreqs();
    }

    @Override
    public boolean hasOffsets() {
      return in.hasOffsets();
    }

    @Override
    public boolean hasPositions() {
      return in.hasPositions();
    }
    
    @Override
    public boolean hasPayloads() {
      return in.hasPayloads();
    }

    @Override
    public Object getStats() throws IOException {
      return in.getStats();
    }
  }

  /** Base class for filtering {@link TermsEnum} implementations. */
  public static class FilterTermsEnum extends TermsEnum {
    /** The underlying TermsEnum instance. */
    protected final TermsEnum in;

    /**
     * Creates a new FilterTermsEnum
     * @param in the underlying TermsEnum instance.
     */
    public FilterTermsEnum(TermsEnum in) {
      if (in == null) {
        throw new NullPointerException("incoming TermsEnum cannot be null");
      }
      this.in = in;
    }

    @Override
    public AttributeSource attributes() {
      return in.attributes();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      return in.seekCeil(text);
    }

    @Override
    public void seekExact(long ord) throws IOException {
      in.seekExact(ord);
    }

    @Override
    public BytesRef next() throws IOException {
      return in.next();
    }

    @Override
    public BytesRef term() throws IOException {
      return in.term();
    }

    @Override
    public long ord() throws IOException {
      return in.ord();
    }

    @Override
    public int docFreq() throws IOException {
      return in.docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
      return in.totalTermFreq();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      return in.postings(reuse, flags);
    }

  }

  /** Base class for filtering {@link PostingsEnum} implementations. */
  public static class FilterPostingsEnum extends PostingsEnum {
    /** The underlying PostingsEnum instance. */
    protected final PostingsEnum in;

    /**
     * Create a new FilterPostingsEnum
     * @param in the underlying PostingsEnum instance.
     */
    public FilterPostingsEnum(PostingsEnum in) {
      if (in == null) {
        throw new NullPointerException("incoming PostingsEnum cannot be null");
      }
      this.in = in;
    }

    @Override
    public AttributeSource attributes() {
      return in.attributes();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int freq() throws IOException {
      return in.freq();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public int nextPosition() throws IOException {
      return in.nextPosition();
    }

    @Override
    public int startOffset() throws IOException {
      return in.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return in.endOffset();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return in.getPayload();
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  /** The underlying LeafReader. */
  protected final LeafReader in;

  /**
   * <p>Construct a FilterLeafReader based on the specified base reader.
   * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
   * @param in specified base reader.
   */
  public FilterLeafReader(LeafReader in) {
    super();
    if (in == null) {
      throw new NullPointerException("incoming LeafReader cannot be null");
    }
    this.in = in;
    in.registerParentReader(this);
  }

  /**
   * A CoreClosedListener wrapper that adjusts the core cache key that
   * the wrapper is called with. This is useful if the core cache key
   * of a reader is different from the key of the wrapped reader.
   */
  private static class CoreClosedListenerWrapper implements CoreClosedListener {

    public static CoreClosedListener wrap(CoreClosedListener listener, Object thisCoreKey, Object inCoreKey) {
      if (thisCoreKey == inCoreKey) {
        // this reader has the same core cache key as its parent, nothing to do
        return listener;
      } else {
        // we don't have the same cache key as the wrapped reader, we need to wrap
        // the listener to call it with the correct cache key
        return new CoreClosedListenerWrapper(listener, thisCoreKey, inCoreKey);
      }
    }

    private final CoreClosedListener in;
    private final Object thisCoreKey;
    private final Object inCoreKey;

    private CoreClosedListenerWrapper(CoreClosedListener in, Object thisCoreKey, Object inCoreKey) {
      this.in = in;
      this.thisCoreKey = thisCoreKey;
      this.inCoreKey = inCoreKey;
    }

    @Override
    public void onClose(Object ownerCoreCacheKey) throws IOException {
      assert inCoreKey == ownerCoreCacheKey;
      in.onClose(thisCoreKey);
    }

    // NOTE: equals/hashcore are important for removeCoreClosedListener to work
    // correctly

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != CoreClosedListenerWrapper.class) {
        return false;
      }
      CoreClosedListenerWrapper that = (CoreClosedListenerWrapper) obj;
      return in.equals(that.in) && thisCoreKey == that.thisCoreKey;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), in, thisCoreKey);
    }

  }

  @Override
  public void addCoreClosedListener(final CoreClosedListener listener) {
    in.addCoreClosedListener(CoreClosedListenerWrapper.wrap(listener, getCoreCacheKey(), in.getCoreCacheKey()));
  }

  @Override
  public void removeCoreClosedListener(CoreClosedListener listener) {
    in.removeCoreClosedListener(CoreClosedListenerWrapper.wrap(listener, getCoreCacheKey(), in.getCoreCacheKey()));
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return in.getLiveDocs();
  }
  
  @Override
  public FieldInfos getFieldInfos() {
    return in.getFieldInfos();
  }

  @Override
  public Fields getTermVectors(int docID)
          throws IOException {
    ensureOpen();
    return in.getTermVectors(docID);
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.maxDoc();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    in.document(docID, visitor);
  }

  @Override
  protected void doClose() throws IOException {
    in.close();
  }
  
  @Override
  public Fields fields() throws IOException {
    ensureOpen();
    return in.fields();
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("FilterLeafReader(");
    buffer.append(in);
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    return in.getNumericDocValues(field);
  }
  
  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    return in.getBinaryDocValues(field);
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedDocValues(field);
  }
  
  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedNumericDocValues(field);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedSetDocValues(field);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    return in.getNormValues(field);
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    ensureOpen();
    return in.getDocsWithField(field);
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    in.checkIntegrity();
  }

  /** Returns the wrapped {@link LeafReader}. */
  public LeafReader getDelegate() {
    return in;
  }
}
