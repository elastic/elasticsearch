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

package org.elasticsearch.action.admin.indices.seal;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;

import java.util.Arrays;

/**
 * A request to seal one or more indices.
 */
public class SealIndicesRequest extends BroadcastRequest {

    SealIndicesRequest() {
    }

    /**
     * Constructs a seal request against one or more indices. If nothing is provided, all indices will
     * be sealed.
     */
    public SealIndicesRequest(String... indices) {
        super(indices);
    }

    @Override
    public String toString() {
        return "SealIndicesRequest{" +
                "indices=" + Arrays.toString(indices) +
                ", indicesOptions=" + indicesOptions() +
                '}';
    }
}
