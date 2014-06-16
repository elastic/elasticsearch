/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.optimize;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;

/**
 * A request to optimize one or more indices. In order to optimize on all the indices, pass an empty array or
 * <tt>null</tt> for the indices.
 * <p/>
 * <p>{@link #setWaitForMerge(boolean)} allows to control if the call will block until the optimize completes and
 * defaults to <tt>true</tt>.
 * <p/>
 * <p>{@link #setMaxNumSegments(int)} allows to control the number of segments to optimize down to. By default, will
 * cause the optimize process to optimize down to half the configured number of segments.
 */
public class OptimizeRequestBuilder extends BroadcastOperationRequestBuilder<OptimizeRequest, OptimizeResponse, OptimizeRequestBuilder, IndicesAdminClient> {

    public OptimizeRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new OptimizeRequest());
    }

    /**
     * Should the call block until the optimize completes. Defaults to <tt>true</tt>.
     */
    public OptimizeRequestBuilder setWaitForMerge(boolean waitForMerge) {
        request.waitForMerge(waitForMerge);
        return this;
    }

    /**
     * Will optimize the index down to <= maxNumSegments. By default, will cause the optimize
     * process to optimize down to half the configured number of segments.
     */
    public OptimizeRequestBuilder setMaxNumSegments(int maxNumSegments) {
        request.maxNumSegments(maxNumSegments);
        return this;
    }

    /**
     * Should the optimization only expunge deletes from the index, without full optimization.
     * Defaults to full optimization (<tt>false</tt>).
     */
    public OptimizeRequestBuilder setOnlyExpungeDeletes(boolean onlyExpungeDeletes) {
        request.onlyExpungeDeletes(onlyExpungeDeletes);
        return this;
    }

    /**
     * Should flush be performed after the optimization. Defaults to <tt>true</tt>.
     */
    public OptimizeRequestBuilder setFlush(boolean flush) {
        request.flush(flush);
        return this;
    }

    /**
     * Should the merge be forced even if there is a single segment with no deletions in the shard.
     * Defaults to <tt>false</tt>.
     */
    public OptimizeRequestBuilder setForce(boolean force) {
        request.force(force);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<OptimizeResponse> listener) {
        client.optimize(request, listener);
    }
}
