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
package org.apache.lucene5_shaded.search.spans;


import java.io.IOException;

import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.Term;

/**
 * An interface defining the collection of postings information from the leaves
 * of a {@link Spans}
 *
 * @lucene.experimental
 */
public interface SpanCollector {

  /**
   * Collect information from postings
   * @param postings a {@link PostingsEnum}
   * @param position the position of the PostingsEnum
   * @param term     the {@link Term} for this postings list
   * @throws IOException on error
   */
  public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException;

  /**
   * Call to indicate that the driving Spans has moved to a new position
   */
  public void reset();

}
