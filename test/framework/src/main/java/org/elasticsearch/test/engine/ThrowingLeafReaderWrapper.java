/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.engine;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

/**
 * An FilterLeafReader that allows to throw exceptions if certain methods
 * are called on is. This allows to test parts of the system under certain
 * error conditions that would otherwise not be possible.
 */
public class ThrowingLeafReaderWrapper extends SequentialStoredFieldsLeafReader {

    private final Thrower thrower;

    /**
     * Flags passed to {@link Thrower#maybeThrow(org.elasticsearch.test.engine.ThrowingLeafReaderWrapper.Flags)}
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
     * methods called on the IndexReader that is wrapped by {@link ThrowingLeafReaderWrapper}
     */
    public interface Thrower {
        /**
         * Maybe throws an exception ;)
         */
        void maybeThrow(Flags flag) throws IOException;

        /**
         * If this method returns true the {@link Terms} instance for the given field
         * is wrapped with Thrower support otherwise no exception will be thrown for
         * the current {@link Terms} instance or any other instance obtained from it.
         */
        boolean wrapTerms(String field);
    }

    public ThrowingLeafReaderWrapper(LeafReader in, Thrower thrower) {
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
        public TermsEnum iterator() throws IOException {
            TermsEnum termsEnum = super.iterator();
            thrower.maybeThrow(Flags.TermsEnum);
            return new ThrowingTermsEnum(termsEnum, thrower);
        }
    }

    static class ThrowingTermsEnum extends FilterTermsEnum {
        private final Thrower thrower;

        ThrowingTermsEnum(TermsEnum in, Thrower thrower) {
            super(in);
            this.thrower = thrower;

        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            if ((flags & PostingsEnum.POSITIONS) != 0) {
                thrower.maybeThrow(Flags.DocsAndPositionsEnum);
            } else {
                thrower.maybeThrow(Flags.DocsEnum);
            }
            return super.postings(reuse, flags);
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


    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }
}
