/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Map;

/**
 * This is a quality of life class that adds synthetic source context for text fields that need it.
 */
public abstract class TextFamilyFieldType extends StringFieldType {

    public static final String FALLBACK_FIELD_NAME_SUFFIX = "._original";
    private final boolean isSyntheticSourceEnabled;
    private final boolean isWithinMultiField;

    public TextFamilyFieldType(
        String name,
        IndexType indexType,
        boolean isStored,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta,
        boolean isSyntheticSourceEnabled,
        boolean isWithinMultiField
    ) {
        super(name, indexType, isStored, textSearchInfo, meta);
        this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
        this.isWithinMultiField = isWithinMultiField;
    }

    public boolean isSyntheticSourceEnabled() {
        return isSyntheticSourceEnabled;
    }

    public boolean isWithinMultiField() {
        return isWithinMultiField;
    }

    /**
     * Returns the name of the "fallback" field that can be used for synthetic source when the "main" field was not
     * stored for whatever reason.
     */
    public String syntheticSourceFallbackFieldName() {
        return name() + FALLBACK_FIELD_NAME_SUFFIX;
    }

    /**
     * Create an {@link IntervalsSource} for the given term.
     */
    public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create an {@link IntervalsSource} for the given prefix.
     */
    public IntervalsSource prefixIntervals(BytesRef prefix, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a fuzzy {@link IntervalsSource} for the given term.
     */
    public IntervalsSource fuzzyIntervals(
        String term,
        int maxDistance,
        int prefixLength,
        boolean transpositions,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a wildcard {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a regexp {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a range {@link IntervalsSource} for the given ranges
     */
    public IntervalsSource rangeIntervals(
        BytesRef lowerTerm,
        BytesRef upperTerm,
        boolean includeLower,
        boolean includeUpper,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * A {@link BlockLoader.ConditionalBlockLoader} that checks whether the prefer field exists in the _ignore field.
     * If the prefer field's term does not exist in the _ignore field, the prefer loader is used for all documents.
     * If the term exists in the _ignore field, the loader checks each document: if the term appears in the _ignore field for a document,
     * the fallback loader is used for that document; otherwise use the prefer loader.
     */
    public static final class ConditionalBlockLoaderWithIgnoreField extends BlockLoader.ConditionalBlockLoader {
        private LeafReaderContext lastContext;
        private DocIdSetIterator postings;
        private final String preferField;

        public ConditionalBlockLoaderWithIgnoreField(String preferField, BlockLoader preferLoader, BlockLoader fallbackLoader) {
            super(preferLoader, fallbackLoader);
            this.preferField = preferField;
        }

        private DocIdSetIterator loadPostings(LeafReaderContext context) throws IOException {
            if (context.reader().getFieldInfos().fieldInfo(preferField) == null) {
                // the prefer_field missing or hidden; use fallback loader for all docs
                return DocIdSetIterator.all(context.reader().maxDoc());
            }
            Terms terms = context.reader().terms(IgnoredFieldMapper.NAME);
            // the _ignore field might be hidden by FLS, unwrap the leaf reader
            if (terms == null) {
                SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(context.reader());
                if (segmentReader == null) {
                    // can't unwrap the leaf reader so use the fallback for all docs
                    return DocIdSetIterator.all(context.reader().maxDoc());
                }
                terms = segmentReader.terms(IgnoredFieldMapper.NAME);
            }
            if (terms == null) {
                // the _ignore field does not exist, use the prefer loader for all docs
                return null;
            }
            TermsEnum iterator = terms.iterator();
            if (iterator.seekExact(new BytesRef(preferField))) {
                return iterator.postings(null, 0);
            }
            // the prefer field does not exist in the _ignore field, use the prefer loader for all docs
            return null;
        }

        @Override
        protected boolean canUsePreferLoaderForLeaf(LeafReaderContext context) throws IOException {
            if (lastContext != context) {
                lastContext = context;
                postings = loadPostings(context);
                if (postings != null) {
                    postings.nextDoc();
                }
            }
            return postings == null || postings.docID() == DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        protected boolean canUsePreferLoaderForDoc(int docId) throws IOException {
            if (postings == null) {
                return true;
            }
            int current = postings.docID();
            if (current < docId) {
                return postings.advance(docId) > docId;
            } else {
                return current > docId;
            }
        }
    }
}
