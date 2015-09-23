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

package org.elasticsearch.index.analysis;

import java.io.IOException;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeFactory;

/**
 * This tokenizer uses regex pattern matching to construct distinct tokens
 * for the input stream.  It takes two arguments:  "pattern" and "group".
 * <ul>
 * <li>"pattern" is the regular expression.</li>
 * <li>"group" says which group to extract into tokens.</li>
 *  </ul>
 * <p>
 * group=-1 (the default) is equivalent to "split".  In this case, the tokens will
 * be equivalent to the output from (without empty tokens):
 * {@link String#split(java.lang.String)}
 * </p>
 * <p>
 * Using group &gt;= 0 selects the matching group as the token.  For example, if you have:<br>
 * <pre>
 *  pattern = \'([^\']+)\'
 *  group = 0
 *  input = aaa 'bbb' 'ccc'
 *</pre>
 * the output will be two tokens: 'bbb' and 'ccc' (including the ' marks).  With the same input
 * but using group=1, the output would be: bbb and ccc (no ' marks)
 * <p>NOTE: This Tokenizer does not output tokens that are of zero length.</p>
 *
 * <b>This is a modified org.apache.lucene.analysis.pattern.PatternTokenizer that does not reuse
 * a StringBuilder.</b>
 *
 * @see Pattern
 */
public final class PatternTokenizer extends Tokenizer {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private String str = "";
  private int index;
  
  private final int group;
  private final Matcher matcher;

  /** creates a new PatternTokenizer returning tokens from group (-1 for split functionality) */
  public PatternTokenizer(Pattern pattern, int group) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, pattern, group);
  }

  /** creates a new PatternTokenizer returning tokens from group (-1 for split functionality) */
  public PatternTokenizer(AttributeFactory factory, Pattern pattern, int group) {
    super(factory);
    this.group = group;

    // Use "" instead of str so don't consume chars
    // (fillBuffer) from the input on throwing IAE below:
    matcher = pattern.matcher("");

    // confusingly group count depends ENTIRELY on the pattern but is only accessible via matcher
    if (group >= 0 && group > matcher.groupCount()) {
      throw new IllegalArgumentException("invalid group specified: pattern only has: " + matcher.groupCount() + " capturing groups");
    }
  }

  @Override
  public boolean incrementToken() {
    if (index >= str.length()) return false;
    clearAttributes();
    if (group >= 0) {
    
      // match a specific group
      while (matcher.find()) {
        index = matcher.start(group);
        final int endIndex = matcher.end(group);
        if (index == endIndex) continue;       
        termAtt.setEmpty().append(str, index, endIndex);
        offsetAtt.setOffset(correctOffset(index), correctOffset(endIndex));
        return true;
      }
      
      index = Integer.MAX_VALUE; // mark exhausted
      return false;
      
    } else {
    
      // String.split() functionality
      while (matcher.find()) {
        if (matcher.start() - index > 0) {
          // found a non-zero-length token
          termAtt.setEmpty().append(str, index, matcher.start());
          offsetAtt.setOffset(correctOffset(index), correctOffset(matcher.start()));
          index = matcher.end();
          return true;
        }
        
        index = matcher.end();
      }
      
      if (str.length() - index == 0) {
        index = Integer.MAX_VALUE; // mark exhausted
        return false;
      }
      
      termAtt.setEmpty().append(str, index, str.length());
      offsetAtt.setOffset(correctOffset(index), correctOffset(str.length()));
      index = Integer.MAX_VALUE; // mark exhausted
      return true;
    }
  }

  @Override
  public void end() throws IOException {
    super.end();
    final int ofs = correctOffset(str.length());
    offsetAtt.setOffset(ofs, ofs);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    fillBuffer(input);
    matcher.reset(str);
    index = 0;
  }
  
  final char[] buffer = new char[8192];
  private void fillBuffer(Reader input) throws IOException {
    int len;
    StringBuilder sb = new StringBuilder();
    while ((len = input.read(buffer)) > 0) {
      sb.append(buffer, 0, len);
    }
    str = sb.toString();
  }
}
