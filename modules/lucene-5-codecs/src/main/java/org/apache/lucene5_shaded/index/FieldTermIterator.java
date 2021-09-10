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


import org.apache.lucene5_shaded.util.BytesRefIterator;

/** Iterates over terms in across multiple fields.  The caller must
 *  check {@link #field} after each {@link #next} to see if the field
 *  changed, but {@code ==} can be used since the iterator
 *  implementation ensures it will use the same String instance for
 *  a given field. */

abstract class FieldTermIterator implements BytesRefIterator {
  /** Returns current field.  This method should not be called
   *  after iteration is done.  Note that you may use == to
   *  detect a change in field. */
  abstract String field();

  /** Del gen of the current term. */
  // TODO: this is really per-iterator not per term, but when we use MergedPrefixCodedTermsIterator we need to know which iterator we are on
  abstract long delGen();
}

