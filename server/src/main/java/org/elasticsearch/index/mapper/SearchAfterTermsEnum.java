/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * This terms enumeration initializes with a seek to a given term but excludes that term
 * from any results. The problem it addresses is that termsEnum.seekCeil()
 * would work but either leaves us positioned on the seek term (if it exists) or the
 * term after (if the seek term doesn't exist). That complicates any subsequent
 * iteration logic so this class simplifies the pagination use case.
 */
public final class SearchAfterTermsEnum extends FilteredTermsEnum {
    private final BytesRef afterRef;

    public SearchAfterTermsEnum(TermsEnum tenum, BytesRef termText) {
        super(tenum);
        afterRef = termText;
        setInitialSeekTerm(termText);
    }

    @Override
    protected AcceptStatus accept(BytesRef term) {
        return term.equals(afterRef) ? AcceptStatus.NO : AcceptStatus.YES;
    }
}
