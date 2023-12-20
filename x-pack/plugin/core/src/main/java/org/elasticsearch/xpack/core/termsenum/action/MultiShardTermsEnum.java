/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Merges terms and stats from multiple TermEnum classes
 * This does a merge sort, by term text.
 * Adapted from Lucene's MultiTermsEnum and differs in that
 * only next() and term() are supported.
 */
public final class MultiShardTermsEnum {

    private final TermMergeQueue queue;
    private final TermsEnumWithCurrent[] top;

    private int numTop;
    private BytesRef current;
    private Function<Object, Object> termsDecoder;

    private record ShardTermsEnum(TermsEnum termsEnum, Function<Object, Object> termsDecoder) {};

    public static class Builder {

        private final List<ShardTermsEnum> shardTermsEnums = new ArrayList<>();

        void add(TermsEnum termsEnum, Function<Object, Object> termsDecoder) {
            this.shardTermsEnums.add(new ShardTermsEnum(termsEnum, termsDecoder));
        }

        MultiShardTermsEnum build() throws IOException {
            return new MultiShardTermsEnum(shardTermsEnums);
        }

        int size() {
            return shardTermsEnums.size();
        }
    }

    /**
     * @param enums TermsEnums from shards which we should merge
     * @throws IOException Errors accessing data
     **/
    private MultiShardTermsEnum(List<ShardTermsEnum> enums) throws IOException {
        queue = new TermMergeQueue(enums.size());
        top = new TermsEnumWithCurrent[enums.size()];
        numTop = 0;
        queue.clear();
        for (ShardTermsEnum shardEnum : enums) {
            final TermsEnum termsEnum = shardEnum.termsEnum();
            final BytesRef term = termsEnum.next();
            if (term != null) {
                final TermsEnumWithCurrent entry = new TermsEnumWithCurrent();
                entry.current = term;
                entry.termsEnum = termsEnum;
                entry.termsDecoder = shardEnum.termsDecoder();
                queue.add(entry);
            } else {
                // field has no terms
            }
        }
    }

    public String decodedTerm() {
        return this.termsDecoder.apply(current).toString();
    }

    private void pullTop() {
        assert numTop == 0;
        numTop = queue.fillTop(top);
        current = top[0].current;
        termsDecoder = top[0].termsDecoder;
    }

    private void pushTop() throws IOException {
        // call next() on each top, and reorder queue
        for (int i = 0; i < numTop; i++) {
            TermsEnumWithCurrent termsEnum = queue.top();
            termsEnum.current = termsEnum.termsEnum.next();
            if (termsEnum.current == null) {
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

    private static final class TermsEnumWithCurrent {
        private Function<Object, Object> termsDecoder;
        private TermsEnum termsEnum;
        private BytesRef current;
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

        /**
         * Add the {@link #top()} slice as well as all slices that are positioned
         * on the same term to {@code tops} and return how many of them there are.
         */
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
