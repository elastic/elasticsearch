/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.api;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.plugin.api.Extensible;
import org.elasticsearch.plugin.api.Nameable;

/**
 * An analysis component used to create token filters.
 */
@Extensible
public interface TokenFilterFactory extends Nameable {

    /**
     * Transform the specified input TokenStream.
     * @param tokenStream a token stream to be transformed
     * @return transformed token stream
     */
    TokenStream create(TokenStream tokenStream);

    /**
     * Normalize a tokenStream for use in multi-term queries.
     * The default implementation returns a given token stream.
     */
    default TokenStream normalize(TokenStream tokenStream) {
        return tokenStream;
    }

    /**
     * Get the {@link AnalysisMode} this filter is allowed to be used in. The default is
     * {@link AnalysisMode#ALL}. Instances need to override this method to define their
     * own restrictions.
     * @return analysis mode
     */
    default AnalysisMode getAnalysisMode() {
        return AnalysisMode.ALL;
    }

}
