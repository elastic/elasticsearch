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

import java.io.Reader;


/**
 * An analyzer wrapper, that doesn't allow to wrap components or readers.
 * By disallowing it, it means that the thread local resources can be delegated
 * to the delegate analyzer, and not also be allocated on this analyzer.
 * This wrapper class is the base class of all analyzers that just delegate to
 * another analyzer, e.g. per field name.
 * 
 * <p>This solves the problem of per field analyzer wrapper, where it also
 * maintains a thread local per field token stream components, while it can
 * safely delegate those and not also hold these data structures, which can
 * become expensive memory wise.
 * 
 * <p><b>Please note:</b> This analyzer uses a private {@link ReuseStrategy},
 * which is returned by {@link #getReuseStrategy()}. This strategy is used when
 * delegating. If you wrap this analyzer again and reuse this strategy, no
 * delegation is done and the given fallback is used.
 */
public abstract class DelegatingAnalyzerWrapper extends AnalyzerWrapper {
  
  /**
   * Constructor.
   * @param fallbackStrategy is the strategy to use if delegation is not possible
   *  This is to support the common pattern:
   *  {@code new OtherWrapper(thisWrapper.getReuseStrategy())} 
   */
  protected DelegatingAnalyzerWrapper(ReuseStrategy fallbackStrategy) {
    super(new DelegatingReuseStrategy(fallbackStrategy));
    // häckidy-hick-hack, because we cannot call super() with a reference to "this":
    ((DelegatingReuseStrategy) getReuseStrategy()).wrapper = this;
  }
  
  @Override
  protected final TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    return super.wrapComponents(fieldName, components);
  }
  
  @Override
  protected final Reader wrapReader(String fieldName, Reader reader) {
    return super.wrapReader(fieldName, reader);
  }
  
  private static final class DelegatingReuseStrategy extends ReuseStrategy {
    DelegatingAnalyzerWrapper wrapper;
    private final ReuseStrategy fallbackStrategy;
    
    DelegatingReuseStrategy(ReuseStrategy fallbackStrategy) {
      this.fallbackStrategy = fallbackStrategy;
    }
    
    @Override
    public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
      if (analyzer == wrapper) {
        final Analyzer wrappedAnalyzer = wrapper.getWrappedAnalyzer(fieldName);
        return wrappedAnalyzer.getReuseStrategy().getReusableComponents(wrappedAnalyzer, fieldName);
      } else {
        return fallbackStrategy.getReusableComponents(analyzer, fieldName);
      }
    }

    @Override
    public void setReusableComponents(Analyzer analyzer, String fieldName,  TokenStreamComponents components) {
      if (analyzer == wrapper) {
        final Analyzer wrappedAnalyzer = wrapper.getWrappedAnalyzer(fieldName);
        wrappedAnalyzer.getReuseStrategy().setReusableComponents(wrappedAnalyzer, fieldName, components);
      } else {
        fallbackStrategy.setReusableComponents(analyzer, fieldName, components);
      }
    }
  };
  
}