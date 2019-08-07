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

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Request for a reload secure settings action.
 */
public class NodesReloadSecureSettingsRequest extends BaseNodesRequest<NodesReloadSecureSettingsRequest> {

    public NodesReloadSecureSettingsRequest() {
        super((String[]) null);
    }

    public NodesReloadSecureSettingsRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Reload secure settings only on certain nodes, based on the nodes IDs specified. If none are passed, secure settings will be reloaded
     * on all the nodes.
     */
    public NodesReloadSecureSettingsRequest(final String... nodesIds) {
        super(nodesIds);
    }

}
