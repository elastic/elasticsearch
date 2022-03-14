/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.logging.api.core;

import org.elasticsearch.logging.internal.RateLimitingFilterImpl;

public interface RateLimitingFilter extends Filter {
    static RateLimitingFilter createRateLimitingFilter() {
        return new RateLimitingFilterImpl();
    }

    void setUseXOpaqueId(boolean useXOpaqueId);

    void reset();
}
