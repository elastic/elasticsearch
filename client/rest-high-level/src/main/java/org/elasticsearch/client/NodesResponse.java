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

package org.elasticsearch.client;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

/**
 * Base class for responses that are node responses. These responses always contain the cluster
 * name and the {@link NodesResponseHeader}.
 */
public abstract class NodesResponse {

    private final NodesResponseHeader header;
    private final String clusterName;

    protected NodesResponse(NodesResponseHeader header, String clusterName) {
        this.header = header;
        this.clusterName = clusterName;
    }

    /**
     * Get the cluster name associated with all of the nodes.
     *
     * @return Never {@code null}.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Gets information about the number of total, successful and failed nodes the request was run on.
     * Also includes exceptions if relevant.
     */
    public NodesResponseHeader getHeader() {
        return header;
    }

    public static <T extends NodesResponse> void declareCommonNodesResponseParsing(ConstructingObjectParser<T, Void> parser) {
        parser.declareObject(ConstructingObjectParser.constructorArg(), NodesResponseHeader::fromXContent, new ParseField("_nodes"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("cluster_name"));
    }
}
