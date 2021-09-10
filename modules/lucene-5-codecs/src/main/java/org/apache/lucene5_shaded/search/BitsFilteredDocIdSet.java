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
package org.apache.lucene5_shaded.search;

import java.util.Objects;

import org.apache.lucene5_shaded.util.Bits;

/**
 * This implementation supplies a filtered DocIdSet, that excludes all
 * docids which are not in a Bits instance. This is especially useful in
 * {@link Filter} to apply the {@code acceptDocs}
 * passed to {@code getDocIdSet()} before returning the final DocIdSet.
 *
 * @see DocIdSet
 * @see Filter
 * @deprecated This class is not useful internally anymore and will be removed
 * in 6.0
 */
@Deprecated
public final class BitsFilteredDocIdSet extends FilteredDocIdSet {

  private final Bits acceptDocs;
  
  /**
   * Convenience wrapper method: If {@code acceptDocs == null} it returns the original set without wrapping.
   * @param set Underlying DocIdSet. If {@code null}, this method returns {@code null}
   * @param acceptDocs Allowed docs, all docids not in this set will not be returned by this DocIdSet.
   * If {@code null}, this method returns the original set without wrapping.
   */
  public static DocIdSet wrap(DocIdSet set, Bits acceptDocs) {
    return (set == null || acceptDocs == null) ? set : new BitsFilteredDocIdSet(set, acceptDocs);
  }
  
  /**
   * Constructor.
   * @param innerSet Underlying DocIdSet
   * @param acceptDocs Allowed docs, all docids not in this set will not be returned by this DocIdSet
   */
  public BitsFilteredDocIdSet(DocIdSet innerSet, Bits acceptDocs) {
    super(innerSet);
    this.acceptDocs = Objects.requireNonNull(acceptDocs, "Bits must not be null");
  }

  @Override
  protected boolean match(int docid) {
    return acceptDocs.get(docid);
  }

}
