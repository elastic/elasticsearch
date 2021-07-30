/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

public class SortingNumericDocValuesTests extends ESTestCase {

   public void testResize() {
       final int oldSize = Integer.MAX_VALUE - 200;
       final int newSize = Integer.MAX_VALUE - 100;
       // This counter should account for the initialization of the array (size == 1)
       // and the diff between newSize (over-sized) and oldSize.
       final AtomicLong counter = new AtomicLong();
       LongConsumer consumer = value -> {
           long total = counter.addAndGet(value);
           assertThat(total, Matchers.greaterThanOrEqualTo(0L));
       };
       SortingNumericDocValues docValues = new SortingNumericDocValues(consumer) {

           @Override
           protected void growExact(int newValuesLength) {
               // don't grow the array
           }

           /** Get the size of the internal array using a method so we can override it during testing */
           protected int getArrayLength() {
               return oldSize;
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
       docValues.resize(newSize);
       final long diff = ArrayUtil.oversize(newSize, Long.BYTES) - oldSize;
       assertThat(counter.get(), Matchers.equalTo((diff + 1) * Long.BYTES));
   }
}
