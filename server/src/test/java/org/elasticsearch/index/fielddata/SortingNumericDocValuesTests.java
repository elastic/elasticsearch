/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.function.LongConsumer;

public class SortingNumericDocValuesTests extends ESTestCase {

   public void testResize() {
       LongConsumer consumer = new LongConsumer() {
           long total = 0;
           @Override
           public void accept(long value) {
               total += value;
               assertThat(total, Matchers.greaterThanOrEqualTo(0L));
           }
       };
       SortingNumericDocValues docValues = new SortingNumericDocValues(consumer) {

           @Override
           protected void growExact(int newValuesLength) {
               // don't grow the array
           }

           @Override
           public boolean advanceExact(int target) {
               return false;
           }

           @Override
           public int docID() {
               return 0;
           }

           @Override
           public int nextDoc() {
               return 0;
           }

           @Override
           public int advance(int target) {
               return 0;
           }

           @Override
           public long cost() {
               return 0;
           }
       };
       docValues.resize(Integer.MAX_VALUE - 100);
   }
}
