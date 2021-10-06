/* @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/**
 * Merges terms and stats from multiple TermEnum classes
 * This does a merge sort, by term text.
 * Adapted from Lucene's MultiTermsEnum and differs in that:
 * 1) Only next(), term() and docFreq() methods are supported
 * 2) Doc counts are longs not ints.
 *
 */
public final class MultiShardTermsEnum {

    private final TermMergeQueue queue;
    private final TermsEnumWithCurrent[] top;

    private int numTop;
    private BytesRef current;

    /** Sole constructor.
     * @param enums TermsEnums from shards which we should merge
     * @throws IOException Errors accessing data 
     **/
    public MultiShardTermsEnum(TermsEnum[] enums) throws IOException {
        queue = new TermMergeQueue(enums.length);
        top = new TermsEnumWithCurrent[enums.length];
        numTop = 0;
        queue.clear();
        for (int i = 0; i < enums.length; i++) {
            final TermsEnum termsEnum = enums[i];
            final BytesRef term = termsEnum.next();
            if (term != null) {
                final TermsEnumWithCurrent entry = new TermsEnumWithCurrent();
                entry.current = term;
                entry.terms = termsEnum;
                queue.add(entry);
            } else {
                // field has no terms
            }
        }
    }

    public BytesRef term() {
        return current;
    }

    private void pullTop() {
        assert numTop == 0;
        numTop = queue.fillTop(top);
        current = top[0].current;
    }

    private void pushTop() throws IOException {
        // call next() on each top, and reorder queue
        for (int i = 0; i < numTop; i++) {
            TermsEnumWithCurrent top = queue.top();
            top.current = top.terms.next();
            if (top.current == null) {
                queue.pop();
            } else {
                queue.updateTop();
            }
        }
        numTop = 0;
    }

    public BytesRef next() throws IOException {
        pushTop();
        // gather equal top fields
        if (queue.size() > 0) {
            // TODO: we could maybe defer this somewhat costly operation until one of the APIs that
            // needs to see the top is invoked (docFreq, postings, etc.)
            pullTop();
        } else {
            current = null;
        }

        return current;
    }

    public long docFreq() throws IOException {
        long sum = 0;
        for (int i = 0; i < numTop; i++) {
            sum += top[i].terms.docFreq();
        }
        return sum;
    }

    static final class TermsEnumWithCurrent {
        TermsEnum terms;
        public BytesRef current;
    }

    private static final class TermMergeQueue extends PriorityQueue<TermsEnumWithCurrent> {
        final int[] stack;

        TermMergeQueue(int size) {
            super(size);
            this.stack = new int[size];
        }

        @Override
        protected boolean lessThan(TermsEnumWithCurrent termsA, TermsEnumWithCurrent termsB) {
            return termsA.current.compareTo(termsB.current) < 0;
        }

        /** Add the {@link #top()} slice as well as all slices that are positioned
         *  on the same term to {@code tops} and return how many of them there are. */
        int fillTop(TermsEnumWithCurrent[] tops) {
            final int size = size();
            if (size == 0) {
                return 0;
            }
            tops[0] = top();
            int numTop = 1;
            stack[0] = 1;
            int stackLen = 1;

            while (stackLen != 0) {
                final int index = stack[--stackLen];
                final int leftChild = index << 1;
                for (int child = leftChild, end = Math.min(size, leftChild + 1); child <= end; ++child) {
                    TermsEnumWithCurrent te = get(child);
                    if (te.current.equals(tops[0].current)) {
                        tops[numTop++] = te;
                        stack[stackLen++] = child;
                    }
                }
            }
            return numTop;
        }

        private TermsEnumWithCurrent get(int i) {
            return (TermsEnumWithCurrent) getHeapArray()[i];
        }
    }
}
