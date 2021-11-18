/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.IndexSettings;

/**
 * Interface implemented by responses for actions that modify the documents in an index like {@link IndexResponse}, {@link UpdateResponse},
 * and {@link BulkResponse}. Rather than implement this directly most implementers should extend {@link DocWriteResponse}.
 */
public interface WriteResponse {
    /**
     * Mark the response as having forced a refresh? Requests that set {@link WriteRequest#setRefreshPolicy(RefreshPolicy)} to
     * {@link RefreshPolicy#IMMEDIATE} should always mark this as true. Requests that set it to {@link RefreshPolicy#WAIT_UNTIL} will only
     * set this to true if they run out of refresh listener slots (see {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD}).
     */
    void setForcedRefresh(boolean forcedRefresh);
}
