/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Goes directly below a {@link CachingTokenFilter} that buffers the whole analyzed query text, and charges that buffer to the
 * request circuit breaker for as long as it lives.
 * <p>
 * Query parsers such as {@link MatchQueryParser} cache every token of the query text before they check any clause limit, so
 * a long query text is held in full on each search thread that parses it. Nothing else accounts for this buffer, so several
 * threads parsing the same long text at once could exhaust the heap.
 * <p>
 * Charges are batched in {@link #CHARGE_BATCH_BYTES} steps, so short texts never touch the breaker. The charge is released on
 * {@link #close()}, which {@link CachingTokenFilter#close()} calls, since the cache is garbage from then on.
 * <p>
 * It also keeps each cached token from carrying a copy of an unrelated term. Every captured state clones the term attribute,
 * and Lucene's {@code CharTermAttributeImpl#clone} copies whatever bytes the last {@code getBytesRef()} call left in it. On a
 * token stream reused across requests those can belong to a long term from an earlier analysis: in a production heap dump each
 * cached two-character token cost about 4 KB this way. This filter calls {@code getBytesRef()} on every token, so only the
 * token's own bytes are copied. That part can go once Lucene's clone no longer copies those bytes.
 */
public final class QueryTokenCacheAccountingFilter extends TokenFilter {

    /** Bytes accumulated before the breaker is charged. */
    static final long CHARGE_BATCH_BYTES = 64 * 1024;

    private static final long STATE_BYTES = RamUsageEstimator.shallowSizeOfInstance(AttributeSource.State.class);
    private static final long BUILDER_BYTES = RamUsageEstimator.shallowSizeOfInstance(BytesRefBuilder.class);
    private static final long BYTES_REF_BYTES = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

    @Nullable
    private final CircuitBreaker breaker;
    private final String label;
    @Nullable
    private final TermToBytesRefAttribute termAtt;
    @Nullable
    private final CharTermAttribute charTermAtt;

    /** Per-token bytes that do not depend on the term, computed on the first token once the consumer has added its attributes. */
    private long fixedBytesPerToken = -1;
    private long estimatedBytes;
    private long chargedBytes;

    /**
     * @param input the analyzed query text, which the caller then wraps in a {@link CachingTokenFilter}
     * @param breaker the breaker to charge, or {@code null} to only estimate
     * @param field the field the text is analyzed for, named in the breaker label
     */
    public QueryTokenCacheAccountingFilter(TokenStream input, @Nullable CircuitBreaker breaker, String field) {
        super(input);
        this.breaker = breaker;
        this.label = ChildMemoryCircuitBreaker.CATEGORY_QUERY_TOKENS + ":" + field;
        // getAttribute, not addAttribute: callers check whether the stream has a term attribute, and adding one would change that.
        this.termAtt = hasAttribute(TermToBytesRefAttribute.class) ? getAttribute(TermToBytesRefAttribute.class) : null;
        this.charTermAtt = hasAttribute(CharTermAttribute.class) ? getAttribute(CharTermAttribute.class) : null;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (input.incrementToken() == false) {
            return false;
        }
        long bytes = fixedBytesPerToken();
        if (termAtt != null) {
            // Leaves only this token's bytes in the builder, see the class Javadoc.
            int length = termAtt.getBytesRef().length;
            if (length > 0) {
                bytes += RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + ArrayUtil.oversize(length, 1));
            }
        }
        if (charTermAtt != null) {
            bytes += RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * charTermAtt.length()
            );
        }
        estimatedBytes += bytes;
        if (breaker != null && estimatedBytes - chargedBytes >= CHARGE_BATCH_BYTES) {
            long delta = estimatedBytes - chargedBytes;
            breaker.addEstimateBytesAndMaybeBreak(delta, label);
            chargedBytes += delta;
        }
        return true;
    }

    private long fixedBytesPerToken() {
        if (fixedBytesPerToken < 0) {
            // The caching filter's list slot, plus one state node and one cloned attribute per attribute implementation.
            long bytes = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
            for (Iterator<AttributeImpl> it = getAttributeImplsIterator(); it.hasNext();) {
                AttributeImpl impl = it.next();
                bytes += STATE_BYTES + RamUsageEstimator.shallowSizeOf(impl);
                if (impl instanceof CharTermAttributeImpl) {
                    // The clone's new BytesRefBuilder and the BytesRef it holds.
                    bytes += BUILDER_BYTES + BYTES_REF_BYTES;
                } else if (impl instanceof BytesTermAttributeImpl) {
                    // The clone's deep copy of the term's BytesRef.
                    bytes += BYTES_REF_BYTES;
                }
            }
            fixedBytesPerToken = bytes;
        }
        return fixedBytesPerToken;
    }

    /**
     * @return the estimated size of the tokens read so far, charged or not
     */
    long estimatedBytes() {
        return estimatedBytes;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (chargedBytes > 0) {
                breaker.addWithoutBreaking(-chargedBytes, label);
                chargedBytes = 0;
            }
        }
    }
}
