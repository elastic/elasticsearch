/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.elasticsearch.search.diversification.mmr.MMRResultDiversification;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversificationContext;

public final class ResultDiversificationFactory {
    public static ResultDiversification<?> getDiversifier(
        ResultDiversificationType diversificationType,
        ResultDiversificationContext diversificationContext
    ) {
        if (diversificationType == null) {
            throw new IllegalArgumentException("diversification type cannot be null");
        }

        if (diversificationType == ResultDiversificationType.MMR) {
            if (diversificationContext instanceof MMRResultDiversificationContext mmrResultDiversificationContext) {
                return new MMRResultDiversification(mmrResultDiversificationContext);
            }
            throw new IllegalArgumentException("context must be an MMRResultDiversificationContext if using MMR");
        }

        throw new IllegalArgumentException("unsupported diversification type [" + diversificationType + "]");
    }
}
