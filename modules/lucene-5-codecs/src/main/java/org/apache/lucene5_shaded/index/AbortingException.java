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


/** Thrown and caught internally in {@link IndexWriter} methods when an {@code IOException} would cause it to
 *  lose previously indexed documents.  When this happens, the {@link IndexWriter} is forcefully 
 *  closed, using {@link IndexWriter#rollback}). */
class AbortingException extends Exception {
  private AbortingException(Throwable cause) {
    super(cause);
    assert cause instanceof AbortingException == false;
  }

  public static AbortingException wrap(Throwable t) {
    if (t instanceof AbortingException) {
      return (AbortingException) t;
    } else {
      return new AbortingException(t);
    }
  }
}
