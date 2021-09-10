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

/**
 * The calculus of spans.
 * 
 * <p>A span is a <code>&lt;doc,startPosition,endPosition&gt;</code> tuple  that is enumerated by
 *    class {@link org.apache.lucene5_shaded.search.spans.Spans Spans}.
 *  </p>
 * 
 * <p>The following span query operators are implemented:
 * 
 * <ul>
 * 
 * <li>A {@link org.apache.lucene5_shaded.search.spans.SpanTermQuery SpanTermQuery} matches all spans
 *    containing a particular {@link org.apache.lucene5_shaded.index.Term Term}.
 *    This should not be used for terms that are indexed at position Integer.MAX_VALUE.
 * </li>
 * 
 * <li> A {@link org.apache.lucene5_shaded.search.spans.SpanNearQuery SpanNearQuery} matches spans
 * which occur near one another, and can be used to implement things like
 * phrase search (when constructed from {@link org.apache.lucene5_shaded.search.spans.SpanTermQuery}s)
 * and inter-phrase proximity (when constructed from other {@link org.apache.lucene5_shaded.search.spans.SpanNearQuery}s).</li>
 *
 * <li> A {@link org.apache.lucene5_shaded.search.spans.SpanWithinQuery SpanWithinQuery} matches spans
 * which occur inside of another spans. </li>
 *
 * <li> A {@link org.apache.lucene5_shaded.search.spans.SpanContainingQuery SpanContainingQuery} matches spans
 * which contain another spans. </li>
 * 
 * <li>A {@link org.apache.lucene5_shaded.search.spans.SpanOrQuery SpanOrQuery} merges spans from a
 * number of other {@link org.apache.lucene5_shaded.search.spans.SpanQuery}s.</li>
 * 
 * <li>A {@link org.apache.lucene5_shaded.search.spans.SpanNotQuery SpanNotQuery} removes spans
 * matching one {@link org.apache.lucene5_shaded.search.spans.SpanQuery SpanQuery} which overlap (or comes
 * near) another.  This can be used, e.g., to implement within-paragraph
 * search.</li>
 * 
 * <li>A {@link org.apache.lucene5_shaded.search.spans.SpanFirstQuery SpanFirstQuery} matches spans
 * matching <code>q</code> whose end position is less than
 * <code>n</code>.  This can be used to constrain matches to the first
 * part of the document.</li>
 * 
 * <li>A {@link org.apache.lucene5_shaded.search.spans.SpanPositionRangeQuery SpanPositionRangeQuery} is
 * a more general form of SpanFirstQuery that can constrain matches to arbitrary portions of the document.</li>
 * 
 * </ul>
 * 
 * In all cases, output spans are minimally inclusive.  In other words, a
 * span formed by matching a span in x and y starts at the lesser of the
 * two starts and ends at the greater of the two ends.
 * 
 * <p>For example, a span query which matches "John Kerry" within ten
 * words of "George Bush" within the first 100 words of the document
 * could be constructed with:
 * <pre class="prettyprint">
 * SpanQuery john   = new SpanTermQuery(new Term("content", "john"));
 * SpanQuery kerry  = new SpanTermQuery(new Term("content", "kerry"));
 * SpanQuery george = new SpanTermQuery(new Term("content", "george"));
 * SpanQuery bush   = new SpanTermQuery(new Term("content", "bush"));
 * 
 * SpanQuery johnKerry =
 *    new SpanNearQuery(new SpanQuery[] {john, kerry}, 0, true);
 * 
 * SpanQuery georgeBush =
 *    new SpanNearQuery(new SpanQuery[] {george, bush}, 0, true);
 * 
 * SpanQuery johnKerryNearGeorgeBush =
 *    new SpanNearQuery(new SpanQuery[] {johnKerry, georgeBush}, 10, false);
 * 
 * SpanQuery johnKerryNearGeorgeBushAtStart =
 *    new SpanFirstQuery(johnKerryNearGeorgeBush, 100);
 * </pre>
 * 
 * <p>Span queries may be freely intermixed with other Lucene queries.
 * So, for example, the above query can be restricted to documents which
 * also use the word "iraq" with:
 * 
 * <pre class="prettyprint">
 * Query query = new BooleanQuery();
 * query.add(johnKerryNearGeorgeBushAtStart, true, false);
 * query.add(new TermQuery("content", "iraq"), true, false);
 * </pre>
 */
package org.apache.lucene5_shaded.search.spans;
