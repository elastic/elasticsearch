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
package org.apache.lucene5_shaded.search;


import java.io.IOException;

/**
 * Provides a {@link FieldComparator} for custom field sorting.
 *
 * @lucene.experimental
 *
 */
public abstract class FieldComparatorSource {

  /**
   * Creates a comparator for the field in the given index.
   * 
   * @param fieldname
   *          Name of the field to create comparator for.
   * @return FieldComparator.
   * @throws IOException
   *           If an error occurs reading the index.
   */
  public abstract FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed)
      throws IOException;
}
