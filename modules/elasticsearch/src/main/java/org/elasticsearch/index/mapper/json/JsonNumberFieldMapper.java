/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.json;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.util.gnu.trove.TIntObjectHashMap;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class JsonNumberFieldMapper<T extends Number> extends JsonFieldMapper<T> {

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final int PRECISION_STEP = NumericUtils.PRECISION_STEP_DEFAULT;
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public abstract static class Builder<T extends Builder, Y extends JsonNumberFieldMapper> extends JsonFieldMapper.Builder<T, Y> {

        protected int precisionStep = Defaults.PRECISION_STEP;

        public Builder(String name) {
            super(name);
            this.index = Defaults.INDEX;
            this.omitNorms = Defaults.OMIT_NORMS;
            this.omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        public T precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return builder;
        }
    }

    private static final ThreadLocal<TIntObjectHashMap<Deque<CachedNumericTokenStream>>> cachedStreams = new ThreadLocal<TIntObjectHashMap<Deque<CachedNumericTokenStream>>>() {
        @Override protected TIntObjectHashMap<Deque<CachedNumericTokenStream>> initialValue() {
            return new TIntObjectHashMap<Deque<CachedNumericTokenStream>>();
        }
    };

    protected final int precisionStep;

    protected JsonNumberFieldMapper(String name, String indexName, String fullName, int precisionStep,
                                    Field.Index index, Field.Store store,
                                    float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                    Analyzer indexAnalyzer, Analyzer searchAnalyzer) {
        super(name, indexName, fullName, index, store, Field.TermVector.NO, boost, omitNorms, omitTermFreqAndPositions, indexAnalyzer, searchAnalyzer);
        if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
            this.precisionStep = Integer.MAX_VALUE;
        } else {
            this.precisionStep = precisionStep;
        }
    }

    protected abstract int maxPrecisionStep();

    public int precisionStep() {
        return this.precisionStep;
    }

    /**
     * Override the defualt behavior (to return the string, and reutrn the actual Number instance).
     */
    @Override public Object valueForSearch(Fieldable field) {
        return value(field);
    }

    @Override public String valueAsString(Fieldable field) {
        return value(field).toString();
    }

    @Override public abstract int sortType();

    /**
     * Removes a cached numeric token stream. The stream will be returned to the cahed once it is used
     * sicne it implements the end method.
     */
    protected CachedNumericTokenStream popCachedStream(int precisionStep) {
        Deque<CachedNumericTokenStream> deque = cachedStreams.get().get(precisionStep);
        if (deque == null) {
            deque = new ArrayDeque<CachedNumericTokenStream>();
            cachedStreams.get().put(precisionStep, deque);
            deque.add(new CachedNumericTokenStream(new NumericTokenStream(precisionStep), precisionStep));
        }
        if (deque.isEmpty()) {
            deque.add(new CachedNumericTokenStream(new NumericTokenStream(precisionStep), precisionStep));
        }
        return deque.pollFirst();
    }

    /**
     * A wrapper around a numeric stream allowing to reuse it by implementing the end method which returns
     * this stream back to the thread local cache.
     */
    protected static final class CachedNumericTokenStream extends TokenStream {

        private final int precisionStep;

        private final NumericTokenStream numericTokenStream;

        public CachedNumericTokenStream(NumericTokenStream numericTokenStream, int precisionStep) {
            super(numericTokenStream);
            this.numericTokenStream = numericTokenStream;
            this.precisionStep = precisionStep;
        }

        public void end() throws IOException {
            numericTokenStream.end();
        }

        /**
         * Close the input TokenStream.
         */
        public void close() throws IOException {
            numericTokenStream.close();
            cachedStreams.get().get(precisionStep).add(this);
        }

        /**
         * Reset the filter as well as the input TokenStream.
         */
        public void reset() throws IOException {
            numericTokenStream.reset();
        }

        @Override public boolean incrementToken() throws IOException {
            return numericTokenStream.incrementToken();
        }

        public CachedNumericTokenStream setIntValue(int value) {
            numericTokenStream.setIntValue(value);
            return this;
        }

        public CachedNumericTokenStream setLongValue(long value) {
            numericTokenStream.setLongValue(value);
            return this;
        }

        public CachedNumericTokenStream setFloatValue(float value) {
            numericTokenStream.setFloatValue(value);
            return this;
        }

        public CachedNumericTokenStream setDoubleValue(double value) {
            numericTokenStream.setDoubleValue(value);
            return this;
        }
    }
}
