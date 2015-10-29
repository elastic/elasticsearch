/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lucene.search.suggest.xdocument;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.*;
import org.apache.lucene.util.automaton.*;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;

import static org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer.*;

/**
 * Token stream which converts a provided token stream to an automaton.
 * The accepted strings enumeration from the automaton are available through the
 * {@link TermToBytesRefAttribute} attribute
 * The token stream uses a {@link PayloadAttribute} to store
 * a completion's payload (see {@link CompletionTokenStream#setPayload(BytesRef)})
 *
 * @lucene.experimental
 */
public final class CompletionTokenStream extends TokenStream {

  private final PayloadAttribute payloadAttr = addAttribute(PayloadAttribute.class);
  private final BytesRefBuilderTermAttribute bytesAtt = addAttribute(BytesRefBuilderTermAttribute.class);

  final TokenStream inputTokenStream;
  final boolean preserveSep;
  final boolean preservePositionIncrements;
  final int maxGraphExpansions;

  private FiniteStringsIterator finiteStrings;
  private BytesRef payload;
  private CharTermAttribute charTermAttribute;

  /**
   * Creates a token stream to convert <code>input</code> to a token stream
   * of accepted strings by its automaton.
   * <p>
   * The token stream <code>input</code> is converted to an automaton
   * with the default settings of {@link CompletionAnalyzer}
   */
  CompletionTokenStream(TokenStream inputTokenStream) {
    this(inputTokenStream, DEFAULT_PRESERVE_SEP, DEFAULT_PRESERVE_POSITION_INCREMENTS, DEFAULT_MAX_GRAPH_EXPANSIONS);
  }

  CompletionTokenStream(TokenStream inputTokenStream, boolean preserveSep, boolean preservePositionIncrements, int maxGraphExpansions) {
    // Don't call the super(input) ctor - this is a true delegate and has a new attribute source since we consume
    // the input stream entirely in toFiniteStrings(input)
    this.inputTokenStream = inputTokenStream;
    this.preserveSep = preserveSep;
    this.preservePositionIncrements = preservePositionIncrements;
    this.maxGraphExpansions = maxGraphExpansions;
  }

  /**
   * Sets a payload available throughout successive token stream enumeration
   */
  public void setPayload(BytesRef payload) {
    this.payload = payload;
  }

  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    if (finiteStrings == null) {
      Automaton automaton = toAutomaton();
      finiteStrings = new LimitedFiniteStringsIterator(automaton, maxGraphExpansions);
    }

    IntsRef string = finiteStrings.next();
    if (string == null) {
      return false;
    }

    Util.toBytesRef(string, bytesAtt.builder()); // now we have UTF-8
    if (charTermAttribute != null) {
      charTermAttribute.setLength(0);
      charTermAttribute.append(bytesAtt.toUTF16());
    }
    if (payload != null) {
      payloadAttr.setPayload(this.payload);
    }

    return true;
  }

  @Override
  public void end() throws IOException {
    super.end();
    if (finiteStrings == null) {
      inputTokenStream.end();
    }
  }

  @Override
  public void close() throws IOException {
    if (finiteStrings == null) {
      inputTokenStream.close();
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    if (hasAttribute(CharTermAttribute.class)) {
      // we only create this if we really need it to safe the UTF-8 to UTF-16 conversion
      charTermAttribute = getAttribute(CharTermAttribute.class);
    }
    finiteStrings = null;
  }

  /**
   * Converts the token stream to an automaton,
   * treating the transition labels as utf-8
   */
  public Automaton toAutomaton() throws IOException {
    return toAutomaton(false);
  }

  /**
   * Converts the tokenStream to an automaton
   */
  public Automaton toAutomaton(boolean unicodeAware) throws IOException {
    // TODO refactor this
    // maybe we could hook up a modified automaton from TermAutomatonQuery here?
    Automaton automaton = null;
    try {
      // Create corresponding automaton: labels are bytes
      // from each analyzed token, with byte 0 used as
      // separator between tokens:
      final TokenStreamToAutomaton tsta;
      if (preserveSep) {
        tsta = new EscapingTokenStreamToAutomaton((char) SEP_LABEL);
      } else {
        // When we're not preserving sep, we don't steal 0xff
        // byte, so we don't need to do any escaping:
        tsta = new TokenStreamToAutomaton();
      }
      tsta.setPreservePositionIncrements(preservePositionIncrements);
      tsta.setUnicodeArcs(unicodeAware);

      automaton = tsta.toAutomaton(inputTokenStream);
    } finally {
      IOUtils.closeWhileHandlingException(inputTokenStream);
    }

    // TODO: we can optimize this somewhat by determinizing
    // while we convert
    automaton = replaceSep(automaton, preserveSep, SEP_LABEL);
    // This automaton should not blow up during determinize:
    return Operations.determinize(automaton, maxGraphExpansions);
  }

  /**
   * Just escapes the 0xff byte (which we still for SEP).
   */
  private static final class EscapingTokenStreamToAutomaton extends TokenStreamToAutomaton {

    final BytesRefBuilder spare = new BytesRefBuilder();
    private char sepLabel;

    public EscapingTokenStreamToAutomaton(char sepLabel) {
      this.sepLabel = sepLabel;
    }

    @Override
    protected BytesRef changeToken(BytesRef in) {
      int upto = 0;
      for (int i = 0; i < in.length; i++) {
        byte b = in.bytes[in.offset + i];
        if (b == (byte) sepLabel) {
          spare.grow(upto + 2);
          spare.setByteAt(upto++, (byte) sepLabel);
          spare.setByteAt(upto++, b);
        } else {
          spare.grow(upto + 1);
          spare.setByteAt(upto++, b);
        }
      }
      spare.setLength(upto);
      return spare.get();
    }
  }

  // Replaces SEP with epsilon or remaps them if
  // we were asked to preserve them:
  private static Automaton replaceSep(Automaton a, boolean preserveSep, int sepLabel) {

    Automaton result = new Automaton();

    // Copy all states over
    int numStates = a.getNumStates();
    for (int s = 0; s < numStates; s++) {
      result.createState();
      result.setAccept(s, a.isAccept(s));
    }

    // Go in reverse topo sort so we know we only have to
    // make one pass:
    Transition t = new Transition();
    int[] topoSortStates = Operations.topoSortStates(a);
    for (int i = 0; i < topoSortStates.length; i++) {
      int state = topoSortStates[topoSortStates.length - 1 - i];
      int count = a.initTransition(state, t);
      for (int j = 0; j < count; j++) {
        a.getNextTransition(t);
        if (t.min == TokenStreamToAutomaton.POS_SEP) {
          assert t.max == TokenStreamToAutomaton.POS_SEP;
          if (preserveSep) {
            // Remap to SEP_LABEL:
            result.addTransition(state, t.dest, sepLabel);
          } else {
            result.addEpsilon(state, t.dest);
          }
        } else if (t.min == TokenStreamToAutomaton.HOLE) {
          assert t.max == TokenStreamToAutomaton.HOLE;

          // Just remove the hole: there will then be two
          // SEP tokens next to each other, which will only
          // match another hole at search time.  Note that
          // it will also match an empty-string token ... if
          // that's somehow a problem we can always map HOLE
          // to a dedicated byte (and escape it in the
          // input).
          result.addEpsilon(state, t.dest);
        } else {
          result.addTransition(state, t.dest, t.min, t.max);
        }
      }
    }

    result.finishState();

    return result;
  }

  /**
   * Attribute providing access to the term builder and UTF-16 conversion
   */
  private interface BytesRefBuilderTermAttribute extends TermToBytesRefAttribute {
    /**
     * Returns the builder from which the term is derived.
     */
    BytesRefBuilder builder();

    /**
     * Returns the term represented as UTF-16
     */
    CharSequence toUTF16();
  }

  /**
   * Custom attribute implementation for completion token stream
   */
  public static final class BytesRefBuilderTermAttributeImpl extends AttributeImpl implements BytesRefBuilderTermAttribute, TermToBytesRefAttribute {
    private final BytesRefBuilder bytes = new BytesRefBuilder();
    private transient CharsRefBuilder charsRef;

    /**
     * Sole constructor
     * no-op
     */
    public BytesRefBuilderTermAttributeImpl() {
    }

    @Override
    public BytesRefBuilder builder() {
      return bytes;
    }

    @Override
    public BytesRef getBytesRef() {
      return bytes.get();
    }

    @Override
    public void clear() {
      bytes.clear();
    }

    @Override
    public void copyTo(AttributeImpl target) {
      BytesRefBuilderTermAttributeImpl other = (BytesRefBuilderTermAttributeImpl) target;
      other.bytes.copyBytes(bytes);
    }

    @Override
    public AttributeImpl clone() {
      BytesRefBuilderTermAttributeImpl other = new BytesRefBuilderTermAttributeImpl();
      copyTo(other);
      return other;
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(TermToBytesRefAttribute.class, "bytes", getBytesRef());
    }

    @Override
    public CharSequence toUTF16() {
      if (charsRef == null) {
        charsRef = new CharsRefBuilder();
      }
      charsRef.copyUTF8Bytes(getBytesRef());
      return charsRef.get();
    }
  }
}
