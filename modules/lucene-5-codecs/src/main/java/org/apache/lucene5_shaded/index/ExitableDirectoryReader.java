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
package org.apache.lucene5_shaded.index;


import org.apache.lucene5_shaded.index.FilterLeafReader.FilterFields;
import org.apache.lucene5_shaded.index.FilterLeafReader.FilterTerms;
import org.apache.lucene5_shaded.index.FilterLeafReader.FilterTermsEnum;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.automaton.CompiledAutomaton;

import java.io.IOException;


/**
 * The {@link ExitableDirectoryReader} wraps a real index {@link DirectoryReader} and
 * allows for a {@link QueryTimeout} implementation object to be checked periodically
 * to see if the thread should exit or not.  If {@link QueryTimeout#shouldExit()}
 * returns true, an {@link ExitingReaderException} is thrown.
 */
public class ExitableDirectoryReader extends FilterDirectoryReader {
  
  private QueryTimeout queryTimeout;

  /**
   * Exception that is thrown to prematurely terminate a term enumeration.
   */
  @SuppressWarnings("serial")
  public static class ExitingReaderException extends RuntimeException {

    /** Constructor **/
    ExitingReaderException(String msg) {
      super(msg);
    }
  }

  /**
   * Wrapper class for a SubReaderWrapper that is used by the ExitableDirectoryReader.
   */
  public static class ExitableSubReaderWrapper extends SubReaderWrapper {
    private QueryTimeout queryTimeout;

    /** Constructor **/
    public ExitableSubReaderWrapper(QueryTimeout queryTimeout) {
      this.queryTimeout = queryTimeout;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
      return new ExitableFilterAtomicReader(reader, queryTimeout);
    }
  }

  /**
   * Wrapper class for another FilterAtomicReader. This is used by ExitableSubReaderWrapper.
   */
  public static class ExitableFilterAtomicReader extends FilterLeafReader {

    private QueryTimeout queryTimeout;
    
    /** Constructor **/
    public ExitableFilterAtomicReader(LeafReader in, QueryTimeout queryTimeout) {
      super(in);
      this.queryTimeout = queryTimeout;
    }

    @Override
    public Fields fields() throws IOException {
      return new ExitableFields(super.fields(), queryTimeout);
    }
    
    @Override
    public Object getCoreCacheKey() {
      return in.getCoreCacheKey();  
    }
    
    @Override
    public Object getCombinedCoreAndDeletesKey() {
      return in.getCombinedCoreAndDeletesKey();
    }
    
  }

  /**
   * Wrapper class for another Fields implementation that is used by the ExitableFilterAtomicReader.
   */
  public static class ExitableFields extends FilterFields {
    
    private QueryTimeout queryTimeout;

    /** Constructor **/
    public ExitableFields(Fields fields, QueryTimeout queryTimeout) {
      super(fields);
      this.queryTimeout = queryTimeout;
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      }
      return new ExitableTerms(terms, queryTimeout);
    }
  }

  /**
   * Wrapper class for another Terms implementation that is used by ExitableFields.
   */
  public static class ExitableTerms extends FilterTerms {

    private QueryTimeout queryTimeout;
    
    /** Constructor **/
    public ExitableTerms(Terms terms, QueryTimeout queryTimeout) {
      super(terms);
      this.queryTimeout = queryTimeout;
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new ExitableTermsEnum(in.intersect(compiled, startTerm), queryTimeout);
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new ExitableTermsEnum(in.iterator(), queryTimeout);
    }
  }

  /**
   * Wrapper class for TermsEnum that is used by ExitableTerms for implementing an
   * exitable enumeration of terms.
   */
  public static class ExitableTermsEnum extends FilterTermsEnum {

    private QueryTimeout queryTimeout;
    
    /** Constructor **/
    public ExitableTermsEnum(TermsEnum termsEnum, QueryTimeout queryTimeout) {
      super(termsEnum);
      this.queryTimeout = queryTimeout;
      checkAndThrow();
    }

    /**
     * Throws {@link ExitingReaderException} if {@link QueryTimeout#shouldExit()} returns true,
     * or if {@link Thread#interrupted()} returns true.
     */
    private void checkAndThrow() {
      if (queryTimeout.shouldExit()) {
        throw new ExitingReaderException("The request took too long to iterate over terms. Timeout: " 
            + queryTimeout.toString()
            + ", TermsEnum=" + in
        );
      } else if (Thread.interrupted()) {
        throw new ExitingReaderException("Interrupted while iterating over terms. TermsEnum=" + in);
      }
    }

    @Override
    public BytesRef next() throws IOException {
      // Before every iteration, check if the iteration should exit
      checkAndThrow();
      return in.next();
    }
  }

  /**
   * Constructor
   * @param in DirectoryReader that this ExitableDirectoryReader wraps around to make it Exitable.
   * @param queryTimeout The object to periodically check if the query should time out.
   */
  public ExitableDirectoryReader(DirectoryReader in, QueryTimeout queryTimeout) throws IOException {
    super(in, new ExitableSubReaderWrapper(queryTimeout));
    this.queryTimeout = queryTimeout;
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    return new ExitableDirectoryReader(in, queryTimeout);
  }

  /**
   * Wraps a provided DirectoryReader. Note that for convenience, the returned reader
   * can be used normally (e.g. passed to {@link DirectoryReader#openIfChanged(DirectoryReader)})
   * and so on.
   */
  public static DirectoryReader wrap(DirectoryReader in, QueryTimeout queryTimeout) throws IOException {
    return new ExitableDirectoryReader(in, queryTimeout);
  }

  @Override
  public String toString() {
    return "ExitableDirectoryReader(" + in.toString() + ")";
  }
}


