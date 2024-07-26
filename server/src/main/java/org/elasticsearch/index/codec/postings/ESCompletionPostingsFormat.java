/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.search.suggest.document.CompletionPostingsFormat;

public class ESCompletionPostingsFormat extends CompletionPostingsFormat {

    //TODO make this configurable via node setting: can it be static?
    private static final FSTLoadMode FST_LOAD_MODE = FSTLoadMode.OFF_HEAP;

    //this is called by SPI
    public ESCompletionPostingsFormat() {
        super("ESCompletion99", FST_LOAD_MODE);
    }

    @Override
    protected PostingsFormat delegatePostingsFormat() {
        return PostingsFormat.forName("Lucene99");
    }
}
