/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under the
 * "Elastic License 2.0", the "GNU Affero General Public License v3.0 only", or the
 * "Server Side Public License v 1"; you may not use this file except in
 * compliance with, at your election, the "Elastic License 2.0", the "GNU Affero
 * General Public License v3.0 only", or the "Server Side Public License v 1".
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.uhighlight.OffsetsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Bounds the length of the match term seen downstream. For multi-term queries the highlighter's
 * automaton is labelled with the query text, so an offsets enum for e.g. a large {@code terms}
 * query reports the whole query string as its term. {@code PassageScorer} feeds every term into a
 * {@code BytesRefHash}, which rejects entries over {@link IndexWriter#MAX_TERM_LENGTH}; without a
 * bound the exception fails the whole search instead of just degrading the highlight.
 *
 * <p>A term past the bound is replaced by a stable digest of itself: unique terms still count as
 * unique, and the highlighted text is unchanged because the term never enters the passage content.
 */
class BoundedTermOffsetsEnum extends OffsetsEnum {
    private final OffsetsEnum delegate;
    private BytesRef boundedTerm;

    BoundedTermOffsetsEnum(OffsetsEnum delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean nextPosition() throws IOException {
        return delegate.nextPosition();
    }

    @Override
    public int freq() throws IOException {
        return delegate.freq();
    }

    @Override
    public BytesRef getTerm() throws IOException {
        BytesRef term = delegate.getTerm();
        if (term == null || term.length <= IndexWriter.MAX_TERM_LENGTH) {
            return term;
        }
        // An offsets enum holds a single term, so its digest is computed once even when the
        // scorer asks for the term of every match position.
        if (boundedTerm == null) {
            boundedTerm = digestOf(term);
        }
        return boundedTerm;
    }

    @Override
    public int startOffset() throws IOException {
        return delegate.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
        return delegate.endOffset();
    }

    private static BytesRef digestOf(BytesRef term) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM must provide MD5", e);
        }
        md.update(term.bytes, term.offset, term.length);
        return new BytesRef(md.digest());
    }
}
