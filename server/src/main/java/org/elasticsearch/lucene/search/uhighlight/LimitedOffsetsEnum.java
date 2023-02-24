/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.search.uhighlight.OffsetsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class LimitedOffsetsEnum extends OffsetsEnum {
    private final OffsetsEnum delegate;
    private final int maxOffset;

    public LimitedOffsetsEnum(OffsetsEnum delegate, int maxOffset) {
        this.delegate = delegate;
        this.maxOffset = maxOffset;
    }

    @Override
    public boolean nextPosition() throws IOException {
        boolean next = delegate.nextPosition();
        if (next == false) {
            return next;
        }
        if (delegate.startOffset() > maxOffset) {
            return false;
        }
        return next;
    }

    @Override
    public int freq() throws IOException {
        return delegate.freq();
    }

    @Override
    public BytesRef getTerm() throws IOException {
        return delegate.getTerm();
    }

    @Override
    public int startOffset() throws IOException {
        return delegate.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
        return delegate.endOffset();
    }
}
