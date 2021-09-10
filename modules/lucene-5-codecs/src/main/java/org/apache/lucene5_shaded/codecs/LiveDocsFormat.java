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
package org.apache.lucene5_shaded.codecs;


import java.io.IOException;
import java.util.Collection;

import org.apache.lucene5_shaded.index.SegmentCommitInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.MutableBits;

/** Format for live/deleted documents
 * @lucene.experimental */
public abstract class LiveDocsFormat {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected LiveDocsFormat() {
  }

  /** Creates a new MutableBits, with all bits set, for the specified size. */
  public abstract MutableBits newLiveDocs(int size) throws IOException;

  /** Creates a new mutablebits of the same bits set and size of existing. */
  public abstract MutableBits newLiveDocs(Bits existing) throws IOException;

  /** Read live docs bits. */
  public abstract Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException;

  /** Persist live docs bits.  Use {@link
   *  SegmentCommitInfo#getNextDelGen} to determine the
   *  generation of the deletes file you should write to. */
  public abstract void writeLiveDocs(MutableBits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) throws IOException;

  /** Records all files in use by this {@link SegmentCommitInfo} into the files argument. */
  public abstract void files(SegmentCommitInfo info, Collection<String> files) throws IOException;
}
