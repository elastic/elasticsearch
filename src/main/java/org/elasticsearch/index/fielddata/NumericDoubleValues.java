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

package org.elasticsearch.index.fielddata;

/**
 * A per-document numeric value.
 */
public abstract class NumericDoubleValues {

  /** Sole constructor. (For invocation by subclass
   * constructors, typically implicit.) */
  protected NumericDoubleValues() {}

  /**
   * Returns the numeric value for the specified document ID. This must return
   * <tt>0d</tt> if the given doc ID has no value.
   * @param docID document ID to lookup
   * @return numeric value
   */
  public abstract double get(int docID);
}
