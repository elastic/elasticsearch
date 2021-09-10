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
package org.apache.lucene5_shaded.analysis;


import java.io.IOException;
import java.io.Closeable;
import java.lang.reflect.Modifier;

import org.apache.lucene5_shaded.analysis.tokenattributes.PackedTokenAttributeImpl;
import org.apache.lucene5_shaded.document.Document;
import org.apache.lucene5_shaded.document.Field;
import org.apache.lucene5_shaded.index.IndexWriter;
import org.apache.lucene5_shaded.util.Attribute;
import org.apache.lucene5_shaded.util.AttributeFactory;
import org.apache.lucene5_shaded.util.AttributeImpl;
import org.apache.lucene5_shaded.util.AttributeSource;

/**
 * A <code>TokenStream</code> enumerates the sequence of tokens, either from
 * {@link Field}s of a {@link Document} or from query text.
 * <p>
 * This is an abstract class; concrete subclasses are:
 * <ul>
 * <li>{@link Tokenizer}, a <code>TokenStream</code> whose input is a Reader; and
 * <li>{@link TokenFilter}, a <code>TokenStream</code> whose input is another
 * <code>TokenStream</code>.
 * </ul>
 * A new <code>TokenStream</code> API has been introduced with Lucene 2.9. This API
 * has moved from being {@link Token}-based to {@link Attribute}-based. While
 * {@link Token} still exists in 2.9 as a convenience class, the preferred way
 * to store the information of a {@link Token} is to use {@link AttributeImpl}s.
 * <p>
 * <code>TokenStream</code> now extends {@link AttributeSource}, which provides
 * access to all of the token {@link Attribute}s for the <code>TokenStream</code>.
 * Note that only one instance per {@link AttributeImpl} is created and reused
 * for every token. This approach reduces object creation and allows local
 * caching of references to the {@link AttributeImpl}s. See
 * {@link #incrementToken()} for further details.
 * <p>
 * <b>The workflow of the new <code>TokenStream</code> API is as follows:</b>
 * <ol>
 * <li>Instantiation of <code>TokenStream</code>/{@link TokenFilter}s which add/get
 * attributes to/from the {@link AttributeSource}.
 * <li>The consumer calls {@link TokenStream#reset()}.
 * <li>The consumer retrieves attributes from the stream and stores local
 * references to all attributes it wants to access.
 * <li>The consumer calls {@link #incrementToken()} until it returns false
 * consuming the attributes after each call.
 * <li>The consumer calls {@link #end()} so that any end-of-stream operations
 * can be performed.
 * <li>The consumer calls {@link #close()} to release any resource when finished
 * using the <code>TokenStream</code>.
 * </ol>
 * To make sure that filters and consumers know which attributes are available,
 * the attributes must be added during instantiation. Filters and consumers are
 * not required to check for availability of attributes in
 * {@link #incrementToken()}.
 * <p>
 * You can find some example code for the new API in the analysis package level
 * Javadoc.
 * <p>
 * Sometimes it is desirable to capture a current state of a <code>TokenStream</code>,
 * e.g., for buffering purposes (see {@link CachingTokenFilter},
 * TeeSinkTokenFilter). For this usecase
 * {@link AttributeSource#captureState} and {@link AttributeSource#restoreState}
 * can be used.
 * <p>The {@code TokenStream}-API in Lucene is based on the decorator pattern.
 * Therefore all non-abstract subclasses must be final or have at least a final
 * implementation of {@link #incrementToken}! This is checked when Java
 * assertions are enabled.
 */
public abstract class TokenStream extends AttributeSource implements Closeable {
  
  /** Default {@link AttributeFactory} instance that should be used for TokenStreams. */
  public static final AttributeFactory DEFAULT_TOKEN_ATTRIBUTE_FACTORY =
    AttributeFactory.getStaticImplementation(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, PackedTokenAttributeImpl.class);

  /**
   * A TokenStream using the default attribute factory.
   */
  protected TokenStream() {
    super(DEFAULT_TOKEN_ATTRIBUTE_FACTORY);
    assert assertFinal();
  }
  
  /**
   * A TokenStream that uses the same attributes as the supplied one.
   */
  protected TokenStream(AttributeSource input) {
    super(input);
    assert assertFinal();
  }
  
  /**
   * A TokenStream using the supplied AttributeFactory for creating new {@link Attribute} instances.
   */
  protected TokenStream(AttributeFactory factory) {
    super(factory);
    assert assertFinal();
  }
  
  private boolean assertFinal() {
    try {
      final Class<?> clazz = getClass();
      if (!clazz.desiredAssertionStatus())
        return true;
      assert clazz.isAnonymousClass() ||
        (clazz.getModifiers() & (Modifier.FINAL | Modifier.PRIVATE)) != 0 ||
        Modifier.isFinal(clazz.getMethod("incrementToken").getModifiers()) :
        "TokenStream implementation classes or at least their incrementToken() implementation must be final";
      return true;
    } catch (NoSuchMethodException nsme) {
      return false;
    }
  }
  
  /**
   * Consumers (i.e., {@link IndexWriter}) use this method to advance the stream to
   * the next token. Implementing classes must implement this method and update
   * the appropriate {@link AttributeImpl}s with the attributes of the next
   * token.
   * <P>
   * The producer must make no assumptions about the attributes after the method
   * has been returned: the caller may arbitrarily change it. If the producer
   * needs to preserve the state for subsequent calls, it can use
   * {@link #captureState} to create a copy of the current attribute state.
   * <p>
   * This method is called for every token of a document, so an efficient
   * implementation is crucial for good performance. To avoid calls to
   * {@link #addAttribute(Class)} and {@link #getAttribute(Class)},
   * references to all {@link AttributeImpl}s that this stream uses should be
   * retrieved during instantiation.
   * <p>
   * To ensure that filters and consumers know which attributes are available,
   * the attributes must be added during instantiation. Filters and consumers
   * are not required to check for availability of attributes in
   * {@link #incrementToken()}.
   * 
   * @return false for end of stream; true otherwise
   */
  public abstract boolean incrementToken() throws IOException;
  
  /**
   * This method is called by the consumer after the last token has been
   * consumed, after {@link #incrementToken()} returned <code>false</code>
   * (using the new <code>TokenStream</code> API). Streams implementing the old API
   * should upgrade to use this feature.
   * <p>
   * This method can be used to perform any end-of-stream operations, such as
   * setting the final offset of a stream. The final offset of a stream might
   * differ from the offset of the last token eg in case one or more whitespaces
   * followed after the last token, but a WhitespaceTokenizer was used.
   * <p>
   * Additionally any skipped positions (such as those removed by a stopfilter)
   * can be applied to the position increment, or any adjustment of other
   * attributes where the end-of-stream value may be important.
   * <p>
   * If you override this method, always call {@code super.end()}.
   * 
   * @throws IOException If an I/O error occurs
   */
  public void end() throws IOException {
    endAttributes(); // LUCENE-3849: don't consume dirty atts
  }

  /**
   * This method is called by a consumer before it begins consumption using
   * {@link #incrementToken()}.
   * <p>
   * Resets this stream to a clean state. Stateful implementations must implement
   * this method so that they can be reused, just as if they had been created fresh.
   * <p>
   * If you override this method, always call {@code super.reset()}, otherwise
   * some internal state will not be correctly reset (e.g., {@link Tokenizer} will
   * throw {@link IllegalStateException} on further usage).
   */
  public void reset() throws IOException {}
  
  /** Releases resources associated with this stream.
   * <p>
   * If you override this method, always call {@code super.close()}, otherwise
   * some internal state will not be correctly reset (e.g., {@link Tokenizer} will
   * throw {@link IllegalStateException} on reuse).
   */
  @Override
  public void close() throws IOException {}
  
}
