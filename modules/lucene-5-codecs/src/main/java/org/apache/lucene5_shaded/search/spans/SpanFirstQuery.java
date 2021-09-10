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


import org.apache.lucene5_shaded.search.spans.FilterSpans.AcceptStatus;
import org.apache.lucene5_shaded.util.ToStringUtils;

import java.io.IOException;

/**
 * Matches spans near the beginning of a field.
 * <p>
 * This class is a simple extension of {@link SpanPositionRangeQuery} in that it assumes the
 * start to be zero and only checks the end boundary.
 */
public class SpanFirstQuery extends SpanPositionRangeQuery {

  /** Construct a SpanFirstQuery matching spans in <code>match</code> whose end
   * position is less than or equal to <code>end</code>. */
  public SpanFirstQuery(SpanQuery match, int end) {
    super(match, 0, end);
  }

  protected AcceptStatus acceptPosition(Spans spans) throws IOException {
    assert spans.startPosition() != spans.endPosition() : "start equals end: " + spans.startPosition();
    if (spans.startPosition() >= end)
      return AcceptStatus.NO_MORE_IN_CURRENT_DOC;
    else if (spans.endPosition() <= end)
      return AcceptStatus.YES;
    else
      return AcceptStatus.NO;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanFirst(");
    buffer.append(match.toString(field));
    buffer.append(", ");
    buffer.append(end);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

}
