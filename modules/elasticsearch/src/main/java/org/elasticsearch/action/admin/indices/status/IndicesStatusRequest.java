/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.action.support.shards.ShardsOperationRequest;
import org.elasticsearch.action.support.shards.ShardsOperationThreading;
import org.elasticsearch.util.Strings;

/**
 * @author kimchy (Shay Banon)
 */
public class IndicesStatusRequest extends ShardsOperationRequest {

    public IndicesStatusRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public IndicesStatusRequest(String... indices) {
        super(indices);
    }

    @Override public IndicesStatusRequest listenerThreaded(boolean listenerThreaded) {
        super.listenerThreaded(listenerThreaded);
        return this;
    }

    @Override public IndicesStatusRequest operationThreading(ShardsOperationThreading operationThreading) {
        super.operationThreading(operationThreading);
        return this;
    }
}
