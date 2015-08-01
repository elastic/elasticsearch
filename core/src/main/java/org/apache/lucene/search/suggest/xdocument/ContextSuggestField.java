package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * {@link SuggestField} which additionally takes in a set of
 * contexts. Example usage of adding a suggestion with contexts is as follows:
 *
 * <pre class="prettyprint">
 *  document.add(
 *   new ContextSuggestField(name, "suggestion", Arrays.asList("context1", "context2"),  4));
 * </pre>
 *
 * Use {@link ContextQuery} to boost and/or filter suggestions
 * at query-time. Use {@link PrefixCompletionQuery}, {@link RegexCompletionQuery}
 * or {@link FuzzyCompletionQuery} if context boost/filtering
 * are not needed.
 *
 * @lucene.experimental
 */
public class ContextSuggestField extends SuggestField {

  /**
   * Separator used between context value and the suggest field value
   */
  public static final int CONTEXT_SEPARATOR = '\u001D';
  static final byte TYPE = 1;

  private final Set<CharSequence> contexts;

  /**
   * Creates a context-enabled suggest field
   *
   * @param name field name
   * @param value field value to get suggestion on
   * @param weight field weight
   * @param contexts associated contexts
   *
   * @throws IllegalArgumentException if either the name or value is null,
   * if value is an empty string, if the weight is negative, if value or
   * contexts contains any reserved characters
   */
  public ContextSuggestField(String name, String value, int weight, CharSequence... contexts) {
    super(name, value, weight);
    validate(value);
    this.contexts = new HashSet<>((contexts != null) ? contexts.length : 0);
    if (contexts != null) {
      Collections.addAll(this.contexts, contexts);
    }
  }

  /**
   * Expert: Sub-classes can inject contexts at
   * index-time
   */
  protected Iterable<CharSequence> contexts() {
    return contexts;
  }

  @Override
  protected CompletionTokenStream wrapTokenStream(TokenStream stream) {
    for (CharSequence context : contexts()) {
      validate(context);
    }
    PrefixTokenFilter prefixTokenFilter = new PrefixTokenFilter(stream, (char) CONTEXT_SEPARATOR, contexts());
    CompletionTokenStream completionTokenStream;
    if (stream instanceof CompletionTokenStream) {
      completionTokenStream = (CompletionTokenStream) stream;
      completionTokenStream = new CompletionTokenStream(prefixTokenFilter,
              completionTokenStream.preserveSep,
              completionTokenStream.preservePositionIncrements,
              completionTokenStream.maxGraphExpansions);
    } else {
      completionTokenStream = new CompletionTokenStream(prefixTokenFilter);
    }
    return completionTokenStream;
  }

  @Override
  protected byte type() {
    return TYPE;
  }

  /**
   * The {@link PrefixTokenFilter} wraps a {@link TokenStream} and adds a set
   * prefixes ahead. The position attribute will not be incremented for the prefixes.
   */
  private static final class PrefixTokenFilter extends TokenFilter {

    private final char separator;
    private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
    private final Iterable<CharSequence> prefixes;

    private Iterator<CharSequence> currentPrefix;

    /**
     * Create a new {@link PrefixTokenFilter}
     *
     * @param input {@link TokenStream} to wrap
     * @param separator Character used separate prefixes from other tokens
     * @param prefixes {@link Iterable} of {@link CharSequence} which keeps all prefixes
     */
    public PrefixTokenFilter(TokenStream input, char separator, Iterable<CharSequence> prefixes) {
      super(input);
      this.prefixes = prefixes;
      this.currentPrefix = null;
      this.separator = separator;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (currentPrefix != null) {
        if (!currentPrefix.hasNext()) {
          return input.incrementToken();
        } else {
          posAttr.setPositionIncrement(0);
        }
      } else {
        currentPrefix = prefixes.iterator();
        termAttr.setEmpty();
        posAttr.setPositionIncrement(1);
      }
      termAttr.setEmpty();
      if (currentPrefix.hasNext()) {
        termAttr.append(currentPrefix.next());
      }
      termAttr.append(separator);
      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      currentPrefix = null;
    }
  }

  private void validate(final CharSequence value) {
    for (int i = 0; i < value.length(); i++) {
      if (CONTEXT_SEPARATOR == value.charAt(i)) {
        throw new IllegalArgumentException("Illegal value [" + value + "] UTF-16 codepoint [0x"
                + Integer.toHexString((int) value.charAt(i))+ "] at position " + i + " is a reserved character");
      }
    }
  }
}
