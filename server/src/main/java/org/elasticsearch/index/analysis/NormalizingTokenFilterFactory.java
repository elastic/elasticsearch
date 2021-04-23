/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;

/**
 * A TokenFilterFactory that may be used for normalization
 *
 * The default implementation delegates {@link #normalize(TokenStream)} to
 * {@link #create(TokenStream)}}.
 */
public interface NormalizingTokenFilterFactory extends TokenFilterFactory {

    @Override
    default TokenStream normalize(TokenStream tokenStream) {
        return create(tokenStream);
    }

}
