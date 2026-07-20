/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;

/**
 * Runtime (per-row) evaluation of full-text functions on {@code text} expressions that are not index-mapped fields,
 * where there is no Lucene index to query. Instead, each value is analyzed on the fly and the resulting token stream
 * is matched directly against the analyzed query terms.
 * <p>
 * The block-level walking (multivalue any-value semantics, null handling, per-thread scratch) is shared here through
 * a single {@code Text} evaluator; what differs between functions is only how a single value's token stream is
 * matched, expressed as a {@link TokenStreamMatcher} ({@link AnyTermMatcher} for {@code match},
 * {@link PhraseMatcher} for {@code match_phrase}).
 */
public final class RuntimeSearch {

    private static final String CONTENT_FIELD = "content_field";

    private RuntimeSearch() {}

    /**
     * Decides whether a single value's token stream matches the query. The stream is already reset; implementations
     * consume it (typically through {@link TermToBytesRefAttribute}) and must not close it.
     */
    public interface TokenStreamMatcher {
        boolean matches(TokenStream stream) throws IOException;
    }

    /**
     * Matches when any analyzed token equals any of the query terms — the OR semantics of runtime {@code match}.
     */
    public record AnyTermMatcher(Set<BytesRef> queryTerms) implements TokenStreamMatcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            // TODO: Use the operator specified in the query options. For now, we use OR, meaning we stop as soon as
            // we find a match.
            while (stream.incrementToken()) {
                if (queryTerms.contains(term.getBytesRef())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Matches when all query terms appear in order at consecutive token positions (slop 0) — the semantics of
     * runtime {@code match_phrase}.
     */
    public record PhraseMatcher(List<BytesRef> queryTerms) implements TokenStreamMatcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute positionIncrement = stream.addAttribute(PositionIncrementAttribute.class);
            // matched[k] being true means the first k + 1 query terms matched a run of tokens at consecutive
            // positions, ending at the previous token (prev) or at the current one (curr).
            boolean[] prev = new boolean[queryTerms.size()];
            boolean[] curr = new boolean[queryTerms.size()];
            while (stream.incrementToken()) {
                if (positionIncrement.getPositionIncrement() != 1) {
                    // A position gap (or a stacked token) breaks adjacency: with slop 0 a phrase only matches
                    // tokens at consecutive positions.
                    Arrays.fill(prev, false);
                }
                BytesRef token = term.getBytesRef();
                for (int k = queryTerms.size() - 1; k > 0; k--) {
                    curr[k] = prev[k - 1] && ByteMatchers.equals(token, queryTerms.get(k));
                }
                curr[0] = ByteMatchers.equals(token, queryTerms.get(0));
                if (curr[queryTerms.size() - 1]) {
                    return true;
                }
                boolean[] swap = prev;
                prev = curr;
                curr = swap;
            }
            return false;
        }
    }

    /**
     * Analyzes the given query string into the ordered list of its terms. With the standard analyzer (the only one
     * supported for now) tokens are emitted at consecutive positions, so the order of the list is enough to describe
     * a phrase; query-side position gaps cannot happen.
     */
    static List<BytesRef> analyzeTerms(Analyzer analyzer, String query) throws IOException {
        List<BytesRef> terms = new ArrayList<>();

        try (TokenStream stream = analyzer.tokenStream(CONTENT_FIELD, query)) {
            stream.reset();
            TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            while (stream.incrementToken()) {
                terms.add(BytesRef.deepCopyOf(term.getBytesRef()));
            }
            stream.end();
        }
        return terms;
    }

    /**
     * Exact (unanalyzed) value equality, shared by the runtime full-text functions for types that a pushed-down
     * query matches as a single term: {@code keyword} for {@code match} and {@code match_phrase} (and {@code ip},
     * {@code version} etc. for {@code match}, whose query value is converted up front).
     */
    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean processBytesRef(
        @Position int position,
        BytesRefBlock fieldBlock,
        @Fixed BytesRef queryStringBytesRef,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        if (fieldBlock == null) {
            return false;
        }

        return fieldBlock.hasValue(position, queryStringBytesRef, scratch);
    }

    @Evaluator(extraName = "Text", warnExceptions = { IOException.class }, allNullsIsNull = false)
    static boolean processText(
        @Position int position,
        BytesRefBlock fieldBlock,
        @Fixed TokenStreamMatcher matcher,
        @Fixed Analyzer analyzer,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) throws IOException {
        if (fieldBlock == null) {
            return false;
        }

        final var valueCount = fieldBlock.getValueCount(position);
        final var startIndex = fieldBlock.getFirstValueIndex(position);

        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            boolean foundMatch;
            scratch = fieldBlock.getBytesRef(valueIndex, scratch);
            // Because we use the standard analyzer and options cannot be specified, the analyzed token stream can be
            // matched directly. Once we accept options (analyzer, slop), we might need a different execution path,
            // e.g. a Lucene MemoryIndex.
            try (TokenStream stream = analyzer.tokenStream(CONTENT_FIELD, scratch.utf8ToString())) {
                stream.reset();
                foundMatch = matcher.matches(stream);
                stream.end();
            }
            if (foundMatch) {
                return true;
            }
        }
        return false;
    }
}
