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

package org.elasticsearch.test.engine;

import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;

/**
 * An FilterAtomicReader that allows to throw exceptions if certain methods
 * are called on is. This allows to test parts of the system under certain
 * error conditions that would otherwise not be possible.
 */
public class ThrowingAtomicReaderWrapper extends FilterAtomicReader {

    private final Thrower thrower;

    /**
     * Flags passed to {@link Thrower#maybeThrow(org.elasticsearch.test.engine.ThrowingAtomicReaderWrapper.Flags)}
     * when the corresponding method is called.
     */
    public enum Flags {
        TermVectors,
        Terms,
        TermsEnum,
        Intersect,
        DocsEnum,
        DocsAndPositionsEnum,
        Fields,
        Norms, NumericDocValues, BinaryDocValues, SortedDocValues, SortedSetDocValues;
    }

    /**
     * A callback interface that allows to throw certain exceptions for
     * methods called on the IndexReader that is wrapped by {@link ThrowingAtomicReaderWrapper}
     */
    public static interface Thrower {
        /**
         * Maybe throws an exception ;)
         */
        public void maybeThrow(Flags flag) throws IOException;

        /**
         * If this method returns true the {@link Terms} instance for the given field
         * is wrapped with Thrower support otherwise no exception will be thrown for
         * the current {@link Terms} instance or any other instance obtained from it.
         */
        public boolean wrapTerms(String field);
    }

    public ThrowingAtomicReaderWrapper(AtomicReader in, Thrower thrower) {
        super(in);
        this.thrower = thrower;
    }


    @Override
    public Fields fields() throws IOException {
        Fields fields = super.fields();
        thrower.maybeThrow(Flags.Fields);
        return fields == null ? null : new ThrowingFields(fields, thrower);
    }

    @Override
    public Fields getTermVectors(int docID) throws IOException {
        Fields fields = super.getTermVectors(docID);
        thrower.maybeThrow(Flags.TermVectors);
        return fields == null ? null : new ThrowingFields(fields, thrower);
    }

    /**
     * Wraps a Fields but with additional asserts
     */
    public static class ThrowingFields extends FilterFields {
        private final Thrower thrower;

        public ThrowingFields(Fields in, Thrower thrower) {
            super(in);
            this.thrower = thrower;
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = super.terms(field);
            if (thrower.wrapTerms(field)) {
                thrower.maybeThrow(Flags.Terms);
                return terms == null ? null : new ThrowingTerms(terms, thrower);
            }
            return terms;
        }
    }

    /**
     * Wraps a Terms but with additional asserts
     */
    public static class ThrowingTerms extends FilterTerms {
        private final Thrower thrower;

        public ThrowingTerms(Terms in, Thrower thrower) {
            super(in);
            this.thrower = thrower;
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton automaton, BytesRef bytes) throws IOException {
            TermsEnum termsEnum = in.intersect(automaton, bytes);
            thrower.maybeThrow(Flags.Intersect);
            return new ThrowingTermsEnum(termsEnum, thrower);
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            TermsEnum termsEnum = super.iterator(reuse);
            thrower.maybeThrow(Flags.TermsEnum);
            return new ThrowingTermsEnum(termsEnum, thrower);
        }
    }

    static class ThrowingTermsEnum extends FilterTermsEnum {
        private final Thrower thrower;

        public ThrowingTermsEnum(TermsEnum in, Thrower thrower) {
            super(in);
            this.thrower = thrower;

        }

        @Override
        public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
            thrower.maybeThrow(Flags.DocsEnum);
            return super.docs(liveDocs, reuse, flags);
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
            thrower.maybeThrow(Flags.DocsAndPositionsEnum);
            return super.docsAndPositions(liveDocs, reuse, flags);
        }
    }


    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        thrower.maybeThrow(Flags.NumericDocValues);
        return super.getNumericDocValues(field);

    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        thrower.maybeThrow(Flags.BinaryDocValues);
        return super.getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        thrower.maybeThrow(Flags.SortedDocValues);
        return super.getSortedDocValues(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        thrower.maybeThrow(Flags.SortedSetDocValues);
        return super.getSortedSetDocValues(field);
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        thrower.maybeThrow(Flags.Norms);
        return super.getNormValues(field);
    }
}
