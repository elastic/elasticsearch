/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.xcontent;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * Objects that can both render themselves in as json/yaml/etc and can provide a {@link RestStatus} for their response. Usually should be
 * implemented by top level responses sent back to users from REST endpoints.
 */
public interface StatusToXContentObject extends ToXContentObject {

    /**
     * Returns the REST status to make sure it is returned correctly
     */
    RestStatus status();
}
