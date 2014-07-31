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

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;

import java.util.Comparator;

/**
 * Pluggable scorer for {@link org.apache.lucene.search.suggest.analyzing.NRTSuggester}
 *
 * TODO: documentation and simplify
 * @param <T>
 */
public abstract class ContextAwareScorer<T extends Comparable<T>> {

    /**
     * A unique name to identify this instance
     */
    protected abstract String name();

    /**
     * @return configuration for the weight output
     */
    protected abstract OutputConfiguration<T> getOutputConfiguration();

    /**
     *
     * @param context provided score for the current search
     * @param pathCost partial path cost
     * @return true if the path should be pruned, false otherwise
     */
    protected abstract boolean prune(T context, T pathCost);

    /**
     *
     * @param context provided score for the current search
     * @return a comparator used to sort paths
     */
    protected abstract Comparator<T> comparator(T context);

    /**
     *
     * @param context provided score for the current search
     * @param result score of a record
     * @return The score of the result
     */
    protected abstract T score(T context, T result);

    /**
     * TODO
     * @param <T>
     */
    public static interface OutputConfiguration<T extends Comparable<T>> {

        Outputs<T> outputSingleton();

        T encode(T input);

        T decode(T output);
    }

    /**
     * Long based ContextAwareScorer
     */
    public static abstract class LongBased extends ContextAwareScorer<Long> {

        @Override
        protected OutputConfiguration<Long> getOutputConfiguration() {
            return new OutputConfiguration<Long>() {
                @Override
                public Outputs<Long> outputSingleton() {
                    return PositiveIntOutputs.getSingleton();
                }

                @Override
                public Long encode(Long input) {
                    if (input < 0 || input > Integer.MAX_VALUE) {
                        throw new UnsupportedOperationException("cannot encode value: " + input);
                    }
                    return Integer.MAX_VALUE - input;
                }

                @Override
                public Long decode(Long output) {
                    return (Integer.MAX_VALUE - output);
                }
            };
        }
    }

    /**
     * BytesRef based ContextAwareScorer
     */
    public static abstract class BytesBased extends ContextAwareScorer<BytesRef> {

        @Override
        protected OutputConfiguration<BytesRef> getOutputConfiguration() {
            return new OutputConfiguration<BytesRef>() {
                @Override
                public Outputs<BytesRef> outputSingleton() {
                    return ByteSequenceOutputs.getSingleton();
                }

                @Override
                public BytesRef encode(BytesRef input) {
                    return input;
                }

                @Override
                public BytesRef decode(BytesRef output) {
                    return output;
                }
            };
        }
    }

    public static final ContextAwareScorer<Long> DEFAULT = new LongBased() {
        @Override
        protected String name() {
            return "default";
        }

        @Override
        protected boolean prune(Long currentContext, Long pathCost) {
            return false;
        }

        @Override
        protected Comparator<Long> comparator(Long context) {
            return new Comparator<Long>() {
                @Override
                public int compare(Long left, Long right) {
                    return left.compareTo(right);
                }
            };
        }

        @Override
        protected Long score(Long context, Long result) {
            return result;
        }
    };
}
