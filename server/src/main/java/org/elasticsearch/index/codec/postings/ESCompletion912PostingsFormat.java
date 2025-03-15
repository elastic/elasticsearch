/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.search.suggest.document.Completion912PostingsFormat;

/** Identical to that of Lucene's Completion101PostingsFormat, except that we flip the default to FSTLoadMode.OFF_HEAP. */
public class ESCompletion912PostingsFormat extends Completion912PostingsFormat {

    public ESCompletion912PostingsFormat() {
        super(FSTLoadMode.OFF_HEAP);  // we flip the default there, from that of Lucene's impl
    }
}
