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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.MergedIterator;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 * This is useful when you're interacting with an {@link
 * IndexReader} implementation that consists of sequential
 * sub-readers (eg {@link DirectoryReader} or {@link
 * MultiReader}).
 *
 * <p><b>NOTE</b>: for composite readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-LeafReader,
 * instead of using this class.
 *
 * @lucene.experimental
 */

public final class MultiFields extends Fields {
  private final Fields[] subs;
  private final ReaderSlice[] subSlices;
  private final Map<String,Terms> terms = new ConcurrentHashMap<>();

  /** Returns a single {@link Fields} instance for this
   *  reader, merging fields/terms/docs/positions on the
   *  fly.  This method will return null if the reader 
   *  has no postings.
   *
   *  <p><b>NOTE</b>: this is a slow way to access postings.
   *  It's better to get the sub-readers and iterate through them
   *  yourself. */
  public static Fields getFields(IndexReader reader) throws IOException {
    final List<LeafReaderContext> leaves = reader.leaves();
    switch (leaves.size()) {
      case 1:
        // already an atomic reader / reader with one leave
        return leaves.get(0).reader().fields();
      default:
        final List<Fields> fields = new ArrayList<>(leaves.size());
        final List<ReaderSlice> slices = new ArrayList<>(leaves.size());
        for (final LeafReaderContext ctx : leaves) {
          final LeafReader r = ctx.reader();
          final Fields f = r.fields();
          fields.add(f);
          slices.add(new ReaderSlice(ctx.docBase, r.maxDoc(), fields.size()-1));
        }
        if (fields.size() == 1) {
          return fields.get(0);
        } else {
          return new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                         slices.toArray(ReaderSlice.EMPTY_ARRAY));
        }
    }
  }

  /** Returns a single {@link Bits} instance for this
   *  reader, merging live Documents on the
   *  fly.  This method will return null if the reader 
   *  has no deletions.
   *
   *  <p><b>NOTE</b>: this is a very slow way to access live docs.
   *  For example, each Bits access will require a binary search.
   *  It's better to get the sub-readers and iterate through them
   *  yourself. */
  public static Bits getLiveDocs(IndexReader reader) {
    if (reader.hasDeletions()) {
      final List<LeafReaderContext> leaves = reader.leaves();
      final int size = leaves.size();
      assert size > 0 : "A reader with deletions must have at least one leave";
      if (size == 1) {
        return leaves.get(0).reader().getLiveDocs();
      }
      final Bits[] liveDocs = new Bits[size];
      final int[] starts = new int[size + 1];
      for (int i = 0; i < size; i++) {
        // record all liveDocs, even if they are null
        final LeafReaderContext ctx = leaves.get(i);
        liveDocs[i] = ctx.reader().getLiveDocs();
        starts[i] = ctx.docBase;
      }
      starts[size] = reader.maxDoc();
      return new MultiBits(liveDocs, starts, true);
    } else {
      return null;
    }
  }

  /**  This method may return null if the field does not exist.*/
  public static Terms getTerms(IndexReader r, String field) throws IOException {
    return getFields(r).terms(field);
  }
  
  /** Returns {@link PostingsEnum} for the specified field and
   *  term.  This will return null if the field or term does
   *  not exist. */
  public static PostingsEnum getTermDocsEnum(IndexReader r, String field, BytesRef term) throws IOException {
    return getTermDocsEnum(r, field, term, PostingsEnum.FREQS);
  }
  
  /** Returns {@link PostingsEnum} for the specified field and
   *  term, with control over whether freqs are required.
   *  Some codecs may be able to optimize their
   *  implementation when freqs are not required.  This will
   *  return null if the field or term does not exist.  See {@link
   *  TermsEnum#postings(PostingsEnum,int)}.*/
  public static PostingsEnum getTermDocsEnum(IndexReader r, String field, BytesRef term, int flags) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      if (termsEnum.seekExact(term)) {
        return termsEnum.postings(null, flags);
      }
    }
    return null;
  }

  /** Returns {@link PostingsEnum} for the specified
   *  field and term.  This will return null if the field or
   *  term does not exist or positions were not indexed. 
   *  @see #getTermPositionsEnum(IndexReader, String, BytesRef, int) */
  public static PostingsEnum getTermPositionsEnum(IndexReader r, String field, BytesRef term) throws IOException {
    return getTermPositionsEnum(r, field, term, PostingsEnum.ALL);
  }

  /** Returns {@link PostingsEnum} for the specified
   *  field and term, with control over whether offsets and payloads are
   *  required.  Some codecs may be able to optimize
   *  their implementation when offsets and/or payloads are not
   *  required. This will return null if the field or term does not
   *  exist. See {@link TermsEnum#postings(PostingsEnum,int)}. */
  public static PostingsEnum getTermPositionsEnum(IndexReader r, String field, BytesRef term, int flags) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      if (termsEnum.seekExact(term)) {
        return termsEnum.postings(null, flags);
      }
    }
    return null;
  }

  /**
   * Expert: construct a new MultiFields instance directly.
   * @lucene.internal
   */
  // TODO: why is this public?
  public MultiFields(Fields[] subs, ReaderSlice[] subSlices) {
    this.subs = subs;
    this.subSlices = subSlices;
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  @Override
  public Iterator<String> iterator() {
    Iterator<String> subIterators[] = new Iterator[subs.length];
    for(int i=0;i<subs.length;i++) {
      subIterators[i] = subs[i].iterator();
    }
    return new MergedIterator<>(subIterators);
  }

  @Override
  public Terms terms(String field) throws IOException {
    Terms result = terms.get(field);
    if (result != null)
      return result;


    // Lazy init: first time this field is requested, we
    // create & add to terms:
    final List<Terms> subs2 = new ArrayList<>();
    final List<ReaderSlice> slices2 = new ArrayList<>();

    // Gather all sub-readers that share this field
    for(int i=0;i<subs.length;i++) {
      final Terms terms = subs[i].terms(field);
      if (terms != null) {
        subs2.add(terms);
        slices2.add(subSlices[i]);
      }
    }
    if (subs2.size() == 0) {
      result = null;
      // don't cache this case with an unbounded cache, since the number of fields that don't exist
      // is unbounded.
    } else {
      result = new MultiTerms(subs2.toArray(Terms.EMPTY_ARRAY),
          slices2.toArray(ReaderSlice.EMPTY_ARRAY));
      terms.put(field, result);
    }

    return result;
  }

  @Override
  public int size() {
    return -1;
  }

  /** Call this to get the (merged) FieldInfos for a
   *  composite reader. 
   *  <p>
   *  NOTE: the returned field numbers will likely not
   *  correspond to the actual field numbers in the underlying
   *  readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   *  will be unavailable.
   */
  public static FieldInfos getMergedFieldInfos(IndexReader reader) {
    final FieldInfos.Builder builder = new FieldInfos.Builder();
    for(final LeafReaderContext ctx : reader.leaves()) {
      builder.add(ctx.reader().getFieldInfos());
    }
    return builder.finish();
  }

  /** Call this to get the (merged) FieldInfos representing the
   *  set of indexed fields <b>only</b> for a composite reader. 
   *  <p>
   *  NOTE: the returned field numbers will likely not
   *  correspond to the actual field numbers in the underlying
   *  readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   *  will be unavailable.
   */
  public static Collection<String> getIndexedFields(IndexReader reader) {
    final Collection<String> fields = new HashSet<>();
    for(final FieldInfo fieldInfo : getMergedFieldInfos(reader)) {
      if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
        fields.add(fieldInfo.name);
      }
    }
    return fields;
  }
}

