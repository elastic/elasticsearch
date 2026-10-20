/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.SliceIndexing;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Rewrites the Lucene fields of a document so that every term they index starts with the slice the document belongs to.
 * A slice's terms then sort together in the terms dictionary, which makes its postings contiguous on disk: reading a block
 * of postings brings back terms of that slice rather than a slice of every term.
 *
 * <p>This sits on {@link LuceneDocument#add}, the one point every mapper goes through, so a field type that indexes terms
 * is covered without knowing about slices. The rewrite is driven by the Lucene field alone: a field indexes terms when its
 * {@link IndexableFieldType#indexOptions()} is set, and {@link SliceIndexing#prefixesTerms} decides whether its name takes
 * part.
 *
 * <p>The prefix reaches the terms dictionary only. Doc values, points and stored values keep the plain value, which matters
 * because a single Lucene field often feeds several of them from the same accessor: such a field is split into an
 * index-only view carrying the prefix and a plain view carrying everything else.
 */
public final class SliceTermPrefix {

    private final byte[] bytePrefix;
    private final char[] charPrefix;

    public SliceTermPrefix(String slice) {
        final String prefix = SliceIndexing.termPrefix(slice);
        this.bytePrefix = prefix.getBytes(StandardCharsets.US_ASCII);
        this.charPrefix = prefix.toCharArray();
        // The prefix is hex, so prefixing a token stream character by character and a BytesRef byte by byte produce the
        // same term.
        assert charPrefix.length == bytePrefix.length : prefix;
    }

    /**
     * Adds {@code field} to {@code out}, prefixed when it indexes terms. A field that feeds doc values, points or stored
     * values from the same value as its terms contributes two entries.
     */
    void add(List<IndexableField> out, IndexableField field) {
        final IndexableFieldType type = field.fieldType();
        if (type.indexOptions() == IndexOptions.NONE || SliceIndexing.prefixesTerms(field.name()) == false || encodesItsOwnTerms(field)) {
            out.add(field);
            return;
        }
        switch (field.invertableType()) {
            case TOKEN_STREAM ->
                // The prefix goes on the token stream, so the value the field also stores stays plain and no split is needed.
                out.add(new PrefixedTokenStreamField(field));
            case BINARY -> {
                final boolean split = type.stored() || type.docValuesType() != DocValuesType.NONE || type.pointDimensionCount() > 0;
                out.add(new PrefixedBinaryField(field, split ? indexOnly(type) : type));
                if (split) {
                    out.add(new PlainValueField(field));
                }
            }
            default -> throw new AssertionError("unknown way of inverting a field [" + field.invertableType() + "]");
        }
    }

    /**
     * Whether the field lays out terms a query can never ask for by value, which is the {@code completion} field of
     * {@link SliceIndexing#keepsPlainTerms} seen from the indexing side. A {@link FeatureField} is not one of them: its
     * term is the feature name, which {@code SliceTermQueries#singleTermShape} asks for with the same prefix on.
     */
    private static boolean encodesItsOwnTerms(IndexableField field) {
        return field instanceof SuggestField;
    }

    /**
     * The terms of a field whose value Lucene reads as bytes, prefixed. Reads through to the wrapped field so that a mapper
     * holding on to it, as the accumulating doc-values fields do, still decides the value.
     */
    private final class PrefixedBinaryField implements IndexableField {

        private final IndexableField delegate;
        private final IndexableFieldType type;

        PrefixedBinaryField(IndexableField delegate, IndexableFieldType type) {
            this.delegate = delegate;
            this.type = type;
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public IndexableFieldType fieldType() {
            return type;
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }

        @Override
        public BytesRef binaryValue() {
            return SliceIndexing.prefixTerm(bytePrefix, delegate.binaryValue());
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
            return null;
        }

        @Override
        public String stringValue() {
            return null;
        }

        @Override
        public Reader readerValue() {
            return null;
        }

        @Override
        public Number numericValue() {
            return null;
        }

        @Override
        public StoredValue storedValue() {
            return null;
        }
    }

    /**
     * The terms of a field Lucene inverts through an analyzer, prefixed. Everything else is the wrapped field unchanged,
     * including the value it stores.
     */
    private final class PrefixedTokenStreamField implements IndexableField {

        private final IndexableField delegate;

        PrefixedTokenStreamField(IndexableField delegate) {
            this.delegate = delegate;
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public IndexableFieldType fieldType() {
            return delegate.fieldType();
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.TOKEN_STREAM;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
            if (reuse instanceof PrefixingTokenFilter filter) {
                final TokenStream inner = delegate.tokenStream(analyzer, filter.input());
                if (inner == filter.input()) {
                    // Lucene reuses one token stream per field across the documents of a buffer, and consecutive
                    // documents belong to different slices, so the prefix has to be reset rather than inherited.
                    filter.setPrefix(charPrefix);
                    return filter;
                }
                return new PrefixingTokenFilter(inner, charPrefix);
            }
            return new PrefixingTokenFilter(delegate.tokenStream(analyzer, reuse), charPrefix);
        }

        @Override
        public BytesRef binaryValue() {
            return delegate.binaryValue();
        }

        @Override
        public String stringValue() {
            return delegate.stringValue();
        }

        @Override
        public Reader readerValue() {
            return delegate.readerValue();
        }

        @Override
        public Number numericValue() {
            return delegate.numericValue();
        }

        @Override
        public StoredValue storedValue() {
            return delegate.storedValue();
        }
    }

    /**
     * The doc values, points and stored value of a field, with its terms removed. Pairs with {@link PrefixedBinaryField},
     * which carries the terms.
     */
    private static final class PlainValueField implements IndexableField {

        private final IndexableField delegate;
        private final IndexableFieldType type;

        PlainValueField(IndexableField delegate) {
            this.delegate = delegate;
            this.type = notIndexed(delegate.fieldType());
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public IndexableFieldType fieldType() {
            return type;
        }

        @Override
        public InvertableType invertableType() {
            return delegate.invertableType();
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
            return null;
        }

        @Override
        public BytesRef binaryValue() {
            return delegate.binaryValue();
        }

        @Override
        public String stringValue() {
            return delegate.stringValue();
        }

        @Override
        public Reader readerValue() {
            return delegate.readerValue();
        }

        @Override
        public Number numericValue() {
            return delegate.numericValue();
        }

        @Override
        public StoredValue storedValue() {
            return delegate.storedValue();
        }
    }

    /**
     * Prepends the slice to every token. Slice values are ASCII, so the characters this writes are the bytes
     * {@link SliceIndexing#prefixTerm} would have written.
     */
    static final class PrefixingTokenFilter extends TokenFilter {

        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private char[] prefix;

        PrefixingTokenFilter(TokenStream input, char[] prefix) {
            super(input);
            this.prefix = prefix;
        }

        TokenStream input() {
            return input;
        }

        void setPrefix(char[] prefix) {
            this.prefix = prefix;
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (input.incrementToken() == false) {
                return false;
            }
            final int length = termAttribute.length();
            final char[] buffer = termAttribute.resizeBuffer(prefix.length + length);
            System.arraycopy(buffer, 0, buffer, prefix.length, length);
            System.arraycopy(prefix, 0, buffer, 0, prefix.length);
            termAttribute.setLength(prefix.length + length);
            return true;
        }
    }

    /** The field type reduced to what it contributes to the terms dictionary. */
    private static IndexableFieldType indexOnly(IndexableFieldType type) {
        final FieldType indexOnly = new FieldType(type);
        indexOnly.setStored(false);
        indexOnly.setDocValuesType(DocValuesType.NONE);
        indexOnly.setDocValuesSkipIndexType(DocValuesSkipIndexType.NONE);
        indexOnly.setDimensions(0, 0, 0);
        indexOnly.freeze();
        return indexOnly;
    }

    /** The field type with its contribution to the terms dictionary removed. */
    private static IndexableFieldType notIndexed(IndexableFieldType type) {
        final FieldType notIndexed = new FieldType(type);
        notIndexed.setIndexOptions(IndexOptions.NONE);
        notIndexed.setTokenized(false);
        notIndexed.setOmitNorms(true);
        // Term vectors come from the terms, which the indexed view carries.
        notIndexed.setStoreTermVectors(false);
        notIndexed.setStoreTermVectorOffsets(false);
        notIndexed.setStoreTermVectorPositions(false);
        notIndexed.setStoreTermVectorPayloads(false);
        notIndexed.freeze();
        return notIndexed;
    }
}
