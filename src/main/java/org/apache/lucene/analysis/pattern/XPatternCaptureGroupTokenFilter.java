package org.apache.lucene.analysis.pattern;

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
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRef;

/**
 * CaptureGroup uses Java regexes to emit multiple tokens - one for each capture
 * group in one or more patterns.
 *
 * <p>
 * For example, a pattern like:
 * </p>
 *
 * <p>
 * <code>"(https?://([a-zA-Z\-_0-9.]+))"</code>
 * </p>
 *
 * <p>
 * when matched against the string "http://www.foo.com/index" would return the
 * tokens "https://www.foo.com" and "www.foo.com".
 * </p>
 *
 * <p>
 * If none of the patterns match, or if preserveOriginal is true, the original
 * token will be preserved.
 * </p>
 * <p>
 * Each pattern is matched as often as it can be, so the pattern
 * <code> "(...)"</code>, when matched against <code>"abcdefghi"</code> would
 * produce <code>["abc","def","ghi"]</code>
 * </p>
 * <p>
 * A camelCaseFilter could be written as:
 * </p>
 * <p>
 * <code>
 *   "([A-Z]{2,})",                                 <br />
 *   "(?&lt;![A-Z])([A-Z][a-z]+)",                     <br />
 *   "(?:^|\\b|(?&lt;=[0-9_])|(?&lt;=[A-Z]{2}))([a-z]+)", <br />
 *   "([0-9]+)"
 * </code>
 * </p>
 * <p>
 * plus if {@link #preserveOriginal} is true, it would also return
 * <code>"camelCaseFilter</code>
 * </p>
 */
public final class XPatternCaptureGroupTokenFilter extends TokenFilter {

  private final CharTermAttribute charTermAttr = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
  private State state;
  private final Matcher[] matchers;
  private final CharsRef spare = new CharsRef();
  private final int[] groupCounts;
  private final boolean preserveOriginal;
  private int[] currentGroup;
  private int currentMatcher;

  /**
   * @param input
   *          the input {@link TokenStream}
   * @param preserveOriginal
   *          set to true to return the original token even if one of the
   *          patterns matches
   * @param patterns
   *          an array of {@link Pattern} objects to match against each token
   */

  public XPatternCaptureGroupTokenFilter(TokenStream input,
      boolean preserveOriginal, Pattern... patterns) {
    super(input);
    this.preserveOriginal = preserveOriginal;
    this.matchers = new Matcher[patterns.length];
    this.groupCounts = new int[patterns.length];
    this.currentGroup = new int[patterns.length];
    for (int i = 0; i < patterns.length; i++) {
      this.matchers[i] = patterns[i].matcher("");
      this.groupCounts[i] = this.matchers[i].groupCount();
      this.currentGroup[i] = -1;
    }
  }

  private boolean nextCapture() {
    int min_offset = Integer.MAX_VALUE;
    currentMatcher = -1;
    Matcher matcher;

    for (int i = 0; i < matchers.length; i++) {
      matcher = matchers[i];
      if (currentGroup[i] == -1) {
        currentGroup[i] = matcher.find() ? 1 : 0;
      }
      if (currentGroup[i] != 0) {
        while (currentGroup[i] < groupCounts[i] + 1) {
          final int start = matcher.start(currentGroup[i]);
          final int end = matcher.end(currentGroup[i]);
          if (start == end || preserveOriginal && start == 0
              && spare.length == end) {
            currentGroup[i]++;
            continue;
          }
          if (start < min_offset) {
            min_offset = start;
            currentMatcher = i;
          }
          break;
        }
        if (currentGroup[i] == groupCounts[i] + 1) {
          currentGroup[i] = -1;
          i--;
        }
      }
    }
    return currentMatcher != -1;
  }

  @Override
  public boolean incrementToken() throws IOException {

    if (currentMatcher != -1 && nextCapture()) {
      assert state != null;
      clearAttributes();
      restoreState(state);
      final int start = matchers[currentMatcher]
          .start(currentGroup[currentMatcher]);
      final int end = matchers[currentMatcher]
          .end(currentGroup[currentMatcher]);

      posAttr.setPositionIncrement(0);
      charTermAttr.copyBuffer(spare.chars, start, end - start);
      currentGroup[currentMatcher]++;
      return true;
    }

    if (!input.incrementToken()) {
      return false;
    }

    char[] buffer = charTermAttr.buffer();
    int length = charTermAttr.length();
    spare.copyChars(buffer, 0, length);
    state = captureState();

    for (int i = 0; i < matchers.length; i++) {
      matchers[i].reset(spare);
      currentGroup[i] = -1;
    }

    if (preserveOriginal) {
      currentMatcher = 0;
    } else if (nextCapture()) {
      final int start = matchers[currentMatcher]
          .start(currentGroup[currentMatcher]);
      final int end = matchers[currentMatcher]
          .end(currentGroup[currentMatcher]);

      // if we start at 0 we can simply set the length and save the copy
      if (start == 0) {
        charTermAttr.setLength(end);
      } else {
        charTermAttr.copyBuffer(spare.chars, start, end - start);
      }
      currentGroup[currentMatcher]++;
    }
    return true;

  }

  @Override
  public void reset() throws IOException {
    super.reset();
    state = null;
    currentMatcher = -1;
  }

}
