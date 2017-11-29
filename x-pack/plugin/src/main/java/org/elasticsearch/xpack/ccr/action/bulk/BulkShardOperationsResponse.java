/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action.bulk;

import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;

public final class BulkShardOperationsResponse extends ReplicationResponse implements WriteResponse {

    @Override
    public void setForcedRefresh(final boolean forcedRefresh) {

    }

}
