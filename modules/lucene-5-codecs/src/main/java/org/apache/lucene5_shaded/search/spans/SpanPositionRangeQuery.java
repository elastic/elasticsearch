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
 * Checks to see if the {@link #getMatch()} lies between a start and end position
 *
 * See {@link SpanFirstQuery} for a derivation that is optimized for the case where start position is 0.
 */
public class SpanPositionRangeQuery extends SpanPositionCheckQuery {
  protected int start;
  protected int end;

  public SpanPositionRangeQuery(SpanQuery match, int start, int end) {
    super(match);
    this.start = start;
    this.end = end;
  }

  @Override
  protected AcceptStatus acceptPosition(Spans spans) throws IOException {
    assert spans.startPosition() != spans.endPosition();
    AcceptStatus res = (spans.startPosition() >= end)
        ? AcceptStatus.NO_MORE_IN_CURRENT_DOC
        : (spans.startPosition() >= start && spans.endPosition() <= end)
        ? AcceptStatus.YES : AcceptStatus.NO;
    return res;
  }

  /**
   * @return The minimum position permitted in a match
   */
  public int getStart() {
    return start;
  }

  /**
   * @return the maximum end position permitted in a match.
   */
  public int getEnd() {
    return end;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanPosRange(");
    buffer.append(match.toString(field));
    buffer.append(", ").append(start).append(", ");
    buffer.append(end);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (! super.equals(o)) {
      return false;
    }
    SpanPositionRangeQuery other = (SpanPositionRangeQuery)o;
    return this.end == other.end && this.start == other.start;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode() ^ end;
    h = (h * 127) ^ start;
    return h;
  }

}