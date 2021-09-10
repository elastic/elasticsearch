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

/** A TokenFilter is a TokenStream whose input is another TokenStream.
  <p>
  This is an abstract class; subclasses must override {@link #incrementToken()}.
  @see TokenStream
  */
public abstract class TokenFilter extends TokenStream {
  /** The source of tokens for this filter. */
  protected final TokenStream input;

  /** Construct a token stream filtering the given input. */
  protected TokenFilter(TokenStream input) {
    super(input);
    this.input = input;
  }
  
  /** 
   * {@inheritDoc}
   * <p> 
   * <b>NOTE:</b> 
   * The default implementation chains the call to the input TokenStream, so
   * be sure to call <code>super.end()</code> first when overriding this method.
   */
  @Override
  public void end() throws IOException {
    input.end();
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * <b>NOTE:</b> 
   * The default implementation chains the call to the input TokenStream, so
   * be sure to call <code>super.close()</code> when overriding this method.
   */
  @Override
  public void close() throws IOException {
    input.close();
  }

  /**
   * {@inheritDoc}
   * <p>
   * <b>NOTE:</b> 
   * The default implementation chains the call to the input TokenStream, so
   * be sure to call <code>super.reset()</code> when overriding this method.
   */
  @Override
  public void reset() throws IOException {
    input.reset();
  }
}
