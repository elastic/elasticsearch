/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.SliceTermQueries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * Fails a search that asks a slice index for a term without the slice.
 *
 * <p>Queries are built by field types, which apply the prefix (see {@code SliceTermQueries}), so a query that reaches
 * the terms dictionary with a plain term has gone around them. That is a silent wrong-results bug: the term simply does
 * not exist, so the query matches nothing. This reader turns it into a failure instead, and is meant to be installed
 * whenever assertions are on, which makes the test suite the audit of whether anything still bypasses the field type.
 *
 * <p>It only looks; it never rewrites a term. The checks are:
 * <ul>
 *   <li>a term sought by value must carry the prefix of a slice the search targets;</li>
 *   <li>an automaton must accept nothing outside those slices, which it would if it were never confined.</li>
 * </ul>
 */
public final class SliceTermsVerifier extends FilterDirectoryReader {

    /**
     * The reader a search should run against: {@code in} itself, or a view that fails a query reaching the dictionary
     * without the slice.
     *
     * @param keepsPlainTerms the fields a slice index indexes without a prefix (see {@link SliceIndexing#keepsPlainTerms})
     */
    public static IndexReader verifying(
        DirectoryReader in,
        IndexSettings settings,
        SliceSelection slices,
        Predicate<String> keepsPlainTerms
    ) throws IOException {
        if (Assertions.ENABLED == false || settings.isSliceEnabled() == false) {
            return in;
        }
        return new SliceTermsVerifier(in, slices, keepsPlainTerms);
    }

    /** Wraps {@code in} so that a query missing the slice prefix fails, for a field whose terms all carry one. */
    public static DirectoryReader wrap(DirectoryReader in, SliceSelection slices) throws IOException {
        return new SliceTermsVerifier(in, slices, field -> false);
    }

    private final SliceSelection slices;
    private final Predicate<String> keepsPlainTerms;

    private SliceTermsVerifier(DirectoryReader in, SliceSelection slices, Predicate<String> keepsPlainTerms) throws IOException {
        super(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new VerifyingLeafReader(reader, slices, keepsPlainTerms);
            }
        });
        this.slices = slices;
        this.keepsPlainTerms = keepsPlainTerms;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new SliceTermsVerifier(in, slices, keepsPlainTerms);
    }

    /** Unchanged: this reader reports exactly what the one below it reports. */
    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private static final class VerifyingLeafReader extends FilterLeafReader {

        private final byte[][] prefixes;
        private final Predicate<String> keepsPlainTerms;

        VerifyingLeafReader(LeafReader in, SliceSelection slices, Predicate<String> keepsPlainTerms) {
            super(in);
            this.keepsPlainTerms = keepsPlainTerms;
            if (slices.isRestricted() == false) {
                this.prefixes = null;
            } else {
                final List<String> names = slices.names();
                this.prefixes = new byte[names.size()][];
                for (int i = 0; i < names.size(); i++) {
                    this.prefixes[i] = SliceIndexing.termPrefixBytes(names.get(i));
                }
            }
        }

        @Override
        public Terms terms(String field) throws IOException {
            final Terms terms = in.terms(field);
            if (terms == null || SliceIndexing.prefixesTerms(field) == false || keepsPlainTerms.test(field)) {
                return terms;
            }
            return new VerifyingTerms(terms, field, prefixes);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static final class VerifyingTerms extends FilterLeafReader.FilterTerms {

        private final String field;
        private final byte[][] prefixes;

        VerifyingTerms(Terms in, String field, byte[][] prefixes) {
            super(in);
            this.field = field;
            this.prefixes = prefixes;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new VerifyingTermsEnum(in.iterator(), field, prefixes);
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
            if (compiled.type == CompiledAutomaton.AUTOMATON_TYPE.NORMAL && acceptsOutsideSlices(compiled.automaton)) {
                throw unconfined();
            }
            if (startTerm != null) {
                verify(startTerm, field, prefixes);
            }
            return in.intersect(compiled, startTerm);
        }

        /**
         * Whether the automaton accepts any term outside the slices the search targets. A confined one cannot: every
         * term of this field begins with a slice prefix, so the automaton must too.
         */
        private boolean acceptsOutsideSlices(Automaton automaton) {
            final Automaton allowed;
            if (prefixes == null) {
                allowed = Operations.concatenate(List.of(SliceTermQueries.anySlicePrefix(), Automata.makeAnyBinary()));
            } else {
                final List<Automaton> perSlice = new ArrayList<>(prefixes.length);
                for (byte[] prefix : prefixes) {
                    perSlice.add(Operations.concatenate(List.of(Automata.makeBinary(new BytesRef(prefix)), Automata.makeAnyBinary())));
                }
                allowed = Operations.union(perSlice);
            }
            return Operations.isEmpty(
                Operations.determinize(
                    Operations.minus(automaton, allowed, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
                )
            ) == false;
        }

        private IllegalStateException unconfined() {
            return new IllegalStateException(
                "query on field ["
                    + field
                    + "] of a slice index was not restricted to a slice; its terms carry the slice they belong to, so "
                    + "the query must be built through the field type"
            );
        }
    }

    /** The term must carry the prefix of a slice the search targets, or of some slice when it targets all of them. */
    private static void verify(BytesRef term, String field, @Nullable byte[][] prefixes) {
        if (prefixes == null) {
            if (term.length < SliceIndexing.TERM_PREFIX_LENGTH) {
                throw missing(term, field, "any slice");
            }
            return;
        }
        for (byte[] prefix : prefixes) {
            if (startsWith(term, prefix)) {
                return;
            }
        }
        throw missing(term, field, Arrays.stream(prefixes).map(p -> new BytesRef(p).utf8ToString()).toList().toString());
    }

    private static boolean startsWith(BytesRef term, byte[] prefix) {
        if (term.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (term.bytes[term.offset + i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static IllegalStateException missing(BytesRef term, String field, String expected) {
        return new IllegalStateException(
            "term ["
                + term.utf8ToString()
                + "] looked up on field ["
                + field
                + "] of a slice index carries no slice; expected one of ["
                + expected
                + "]. Its terms carry the slice they belong to, so the query must be built through the field type"
        );
    }

    /** Verifies the term on every way of seeking one by value; everything else is the enum below it. */
    private static final class VerifyingTermsEnum extends FilterLeafReader.FilterTermsEnum {

        private final String field;
        private final byte[][] prefixes;

        VerifyingTermsEnum(TermsEnum in, String field, byte[][] prefixes) {
            super(in);
            this.field = field;
            this.prefixes = prefixes;
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            verify(text, field, prefixes);
            return in.seekExact(text);
        }

        @Override
        public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
            verify(text, field, prefixes);
            return in.prepareSeekExact(text);
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            verify(text, field, prefixes);
            return in.seekCeil(text);
        }

        @Override
        public void seekExact(BytesRef term, TermState state) throws IOException {
            verify(term, field, prefixes);
            in.seekExact(term, state);
        }
    }
}
