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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * This is a quality of life class that adds synthetic source context for text fields that need it.
 */
public abstract class TextFamilyFieldType extends StringFieldType {

    public static final String FALLBACK_FIELD_NAME_SUFFIX = "._original";
    private final boolean isSyntheticSourceEnabled;
    private final boolean isWithinMultiField;

    /**
     * Whether this field's doc values keep array order in a column with a companion {@code .counts}, the layout a strictly columnar
     * index uses without the ColumNAR codec. See {@link #existsQuery}.
     */
    protected boolean keepsArrayOrderWithSeparateCounts() {
        return false;
    }

    /**
     * The field is there if the document wrote at least one slot for it, null slots included — which is the rule a strictly
     * columnar index applies whichever format is writing.
     *
     * <p>Under the ColumNAR codec the payload carries its own count and is written for such a document, so the plain doc-values
     * query already answers this. The in-order column writes no value for a document whose slots are all null, only the companion
     * count, so there the count is what says the field is there. Asking the wrong one of the two is how {@code exists} came to
     * disagree between them for {@code f: [null]}.
     */
    @Override
    public Query existsQuery(SearchExecutionContext context) {
        if (keepsArrayOrderWithSeparateCounts()) {
            return new FieldExistsQuery(name() + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX);
        }
        return super.existsQuery(context);
    }

    /**
     * Whether a text query over this field is answered by reading its values, which a field indexing no terms needs.
     * Only the analyzed field types of the family answer this.
     */
    public boolean answersTextQueryFromValues(SearchExecutionContext context) {
        return false;
    }

    /**
     * {@code analyzed} answered by reading this field's values: one query holding every term and condition it asks
     * about, so the values are read and analyzed once however many terms that is.
     */
    public Query toReanalyzingQuery(Query analyzed, SearchExecutionContext context) {
        throw new UnsupportedOperationException("[" + name() + "] does not answer a text query from its values");
    }

    /**
     * A term query over this field's values, or null where its index answers it. The term is looked for among the
     * tokens its values analyze into, as it is where those tokens are indexed.
     */
    @Nullable
    protected final Query termQueryFromValues(Object value, SearchExecutionContext context) {
        return answersTextQueryFromValues(context)
            ? toReanalyzingQuery(new TermQuery(new Term(name(), indexedValueForSearch(value))), context)
            : null;
    }

    /** The same for several terms, as one query, so a document's values are read once for all of them. */
    @Nullable
    protected final Query termsQueryFromValues(Collection<?> values, SearchExecutionContext context) {
        if (answersTextQueryFromValues(context) == false) {
            return null;
        }
        final BooleanQuery.Builder terms = new BooleanQuery.Builder();
        for (Object value : values) {
            terms.add(new TermQuery(new Term(name(), indexedValueForSearch(value))), BooleanClause.Occur.SHOULD);
        }
        return toReanalyzingQuery(terms.build(), context);
    }

    /** A prefix query over the tokens of this field's values, or null where its index answers it. */
    @Nullable
    protected final Query prefixQueryFromValues(String value, boolean caseInsensitive, SearchExecutionContext context) {
        if (answersTextQueryFromValues(context) == false) {
            return null;
        }
        final Term term = new Term(name(), indexedValueForSearch(value));
        return toReanalyzingQuery(caseInsensitive ? AutomatonQueries.caseInsensitivePrefixQuery(term) : new PrefixQuery(term), context);
    }

    /** A wildcard query over the tokens of this field's values, or null where its index answers it. */
    @Nullable
    protected final Query wildcardQueryFromValues(String value, boolean caseInsensitive, SearchExecutionContext context) {
        if (answersTextQueryFromValues(context) == false) {
            return null;
        }
        final Term term = new Term(name(), indexedValueForSearch(value));
        return toReanalyzingQuery(
            caseInsensitive
                ? AutomatonQueries.caseInsensitiveWildcardQuery(term)
                : new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
            context
        );
    }

    /**
     * A wildcard whose pattern the search analyzer normalizes, which is how a {@code query_string} reaches a field.
     * The normalized pattern then looks for a token, as it does where those tokens are indexed.
     */
    @Override
    public Query normalizedWildcardQuery(String value, MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        if (answersTextQueryFromValues(context) == false) {
            return super.normalizedWildcardQuery(value, method, context);
        }
        final Analyzer normalizer = getTextSearchInfo().searchAnalyzer();
        return wildcardQueryFromValues(normalizer == null ? value : normalizeWildcardPattern(name(), value, normalizer), false, context);
    }

    /** A regexp query over the tokens of this field's values, or null where its index answers it. */
    @Nullable
    protected final Query regexpQueryFromValues(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        SearchExecutionContext context
    ) {
        return answersTextQueryFromValues(context)
            ? toReanalyzingQuery(
                new RegexpQuery(new Term(name(), indexedValueForSearch(value)), syntaxFlags, matchFlags, maxDeterminizedStates),
                context
            )
            : null;
    }

    /**
     * Whether {@code query} already reads values. A field that answers every positional query that way wraps its
     * own, and wrapping it again would read the values of a document that holds none.
     */
    protected static boolean isReanalyzing(Query query) {
        final Query inner = query instanceof ConstantScoreQuery constantScore ? constantScore.getQuery() : query;
        return inner instanceof ReanalyzingTextQuery;
    }

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
