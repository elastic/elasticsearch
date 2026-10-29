/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.apache.lucene.util.AttributeFactory;
import org.elasticsearch.index.IndexVersions;

/**
 * {@link ThaiTokenizer} with the pre-Lucene-10.6 buffer-boundary rule. Lucene 10.6 (apache/lucene#16727) started treating any
 * whitespace as a safe place to cut the 1024-char segmenting buffer; before that only line terminators were. On text longer than
 * the buffer without line breaks the two rules cut in different places and so can produce different tokens. Used for indices
 * created before {@link IndexVersions#UPGRADE_TO_LUCENE_10_6_0} so their analysis does not change under them.
 */
final class LegacyThaiTokenizer extends ThaiTokenizer {

    LegacyThaiTokenizer() {
        super();
    }

    LegacyThaiTokenizer(AttributeFactory factory) {
        super(factory);
    }

    @Override
    protected boolean isSafeEnd(char ch) {
        // SegmentingTokenizerBase.isSafeEnd as of Lucene 10.5. Do NOT delegate to super: ThaiTokenizer's
        // override ORs in Character.isWhitespace, which is the behaviour we are excluding.
        return switch (ch) {
            case 0x000D, 0x000A, 0x0085, 0x2028, 0x2029 -> true;
            default -> false;
        };
    }
}
