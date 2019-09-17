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
package org.elasticsearch.client.tasks;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Holder of data created out of a {@link CancelTasksResponse}
 */
public class NodesInfoData {

    private final List<NodeData> nodesInfoData = new ArrayList<>();

    NodesInfoData() {}

    public List<NodeData> getNodesInfoData() {
        return nodesInfoData;
    }

    private void setNodesInfoData(List<NodeData> nodesInfoData) {
        if(nodesInfoData!=null){
            this.nodesInfoData.addAll(nodesInfoData);
        }
    }

    public static final ObjectParser<NodesInfoData, Void> PARSER = new ObjectParser<>("nodes", NodesInfoData::new);

    static {
        PARSER.declareNamedObjects(NodesInfoData::setNodesInfoData, NodeData.PARSER, new ParseField("nodes"));
    }
}
