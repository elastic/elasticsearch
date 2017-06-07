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

package org.elasticsearch.common.util;

/**
 * Base class to compute top terms.
 */
public abstract class StreamingTopK {

    /**
     * The status of an increment.
     */
    public static final class Status {

        /**
         * The bucket the term has been put into. This bucket is a dense integer
         * between 0 and the size of the {@link StreamingTopK} instance. A value
         * of <tt>-1</tt> indicates that the term has been ignored since it is
         * not competitive. As much as possible, {@link StreamingTopK} instances
         * try to reuse the same bucket for the same term.
         */
        public int bucket;

        /**
         * This value will be true if we are recycling a bucket that has
         * previously be used for another term.
         */
        public boolean recycled;

        Status reset(int bucket, boolean recycled) {
            this.bucket = bucket;
            this.recycled = recycled;
            return this;
        }
    }

    /**
     * Increment the count for the given term and sets information about what
     * happened into the {@link Status} object.
     */
    public abstract Status add(long term, Status status);

    /**
     * Return an array that maps bucket ids to their associated term.
     */
    public abstract long[] topTerms();
}
