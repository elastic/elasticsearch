/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * The stored term vectors of a slice index carry the slice on every term, because that is what was indexed. The term vectors
 * API and the highlighters built on it hand terms back to the user, so the slice is removed on the way out. A document
 * belongs to one slice, so all of its terms share a prefix and removing it keeps them in order.
 */
public final class SliceTermVectors {

    private SliceTermVectors() {}

    /** The same fields with the slice removed from the terms of every field that carries one. */
    public static Fields withoutSlice(Fields fields) {
        return fields == null ? null : mapPrefixedTerms(fields, StrippedTerms::new);
    }

    /**
     * A view of the index terms of {@code slice} in which a term is sought by its plain form and reported plain. The term
     * vectors API compares a document's terms against the dictionary to report their statistics, and does so without a
     * field type, so this is where the slice goes on for it.
     */
    public static Fields inSlice(Fields fields, String slice) {
        final byte[] prefix = SliceIndexing.termPrefixBytes(slice);
        return mapPrefixedTerms(fields, terms -> new SliceTerms(terms, prefix));
    }

    /** {@code fields} with {@code view} over the terms of every field that carries a slice, and the rest untouched. */
    private static Fields mapPrefixedTerms(Fields fields, Function<Terms, Terms> view) {
        return new Fields() {
            @Override
            public Iterator<String> iterator() {
                return fields.iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                final Terms terms = fields.terms(field);
                return terms == null || SliceIndexing.prefixesTerms(field) == false ? terms : view.apply(terms);
            }

            @Override
            public int size() {
                return fields.size();
            }
        };
    }

    private static final class StrippedTerms extends FilterLeafReader.FilterTerms {

        StrippedTerms(Terms in) {
            super(in);
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new StrippedTermsEnum(in.iterator());
        }
    }

    private static final class SliceTerms extends FilterLeafReader.FilterTerms {

        private final byte[] prefix;

        SliceTerms(Terms in, byte[] prefix) {
            super(in);
            this.prefix = prefix;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new SliceTermsEnum(in.iterator(), prefix);
        }
    }

    /** Puts the slice on a term on the way in and takes it off on the way out. */
    private static final class SliceTermsEnum extends FilterLeafReader.FilterTermsEnum {

        private final byte[] prefix;

        SliceTermsEnum(TermsEnum in, byte[] prefix) {
            super(in);
            this.prefix = prefix;
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            return in.seekExact(SliceIndexing.prefixTerm(prefix, text));
        }

        @Override
        public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
            return in.prepareSeekExact(SliceIndexing.prefixTerm(prefix, text));
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            return in.seekCeil(SliceIndexing.prefixTerm(prefix, text));
        }

        @Override
        public BytesRef next() throws IOException {
            final BytesRef term = in.next();
            return term == null ? null : SliceIndexing.stripTermPrefix(term);
        }

        @Override
        public BytesRef term() throws IOException {
            return SliceIndexing.stripTermPrefix(in.term());
        }
    }

    private static final class StrippedTermsEnum extends FilterLeafReader.FilterTermsEnum {

        StrippedTermsEnum(TermsEnum in) {
            super(in);
        }

        @Override
        public BytesRef next() throws IOException {
            final BytesRef term = in.next();
            return term == null ? null : SliceIndexing.stripTermPrefix(term);
        }

        @Override
        public BytesRef term() throws IOException {
            return SliceIndexing.stripTermPrefix(in.term());
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            // A document's vector holds one slice, so a lookup by the plain term has to go through the prefixed form,
            // which only the owning slice knows. Callers of a vector enumerate it instead.
            throw new UnsupportedOperationException("term vectors of a slice index cannot be sought by a plain term");
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            throw new UnsupportedOperationException("term vectors of a slice index cannot be sought by a plain term");
        }

        @Override
        public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
            throw new UnsupportedOperationException("term vectors of a slice index cannot be sought by a plain term");
        }
    }
}
