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
package org.elasticsearch.util.yaml.snakeyaml.nodes;

import org.elasticsearch.util.yaml.snakeyaml.error.Mark;

import java.util.List;

/**
 * Represents a sequence.
 * <p>
 * A sequence is a ordered collection of nodes.
 * </p>
 */
public class SequenceNode extends CollectionNode {
    private Class<? extends Object> listType;
    private List<Node> value;

    public SequenceNode(Tag tag, boolean resolved, List<Node> value, Mark startMark, Mark endMark,
                        Boolean flowStyle) {
        super(tag, startMark, endMark, flowStyle);
        if (value == null) {
            throw new NullPointerException("value in a Node is required.");
        }
        this.value = value;
        listType = Object.class;
        this.resolved = resolved;
    }

    public SequenceNode(Tag tag, List<Node> value, Boolean flowStyle) {
        this(tag, true, value, null, null, flowStyle);
    }

    @Override
    public NodeId getNodeId() {
        return NodeId.sequence;
    }

    /**
     * Returns the elements in this sequence.
     *
     * @return Nodes in the specified order.
     */
    public List<Node> getValue() {
        for (Node node : value) {
            node.setType(listType);
        }
        return value;
    }

    public void setListType(Class<? extends Object> listType) {
        this.listType = listType;
    }

    public String toString() {
        return "<" + this.getClass().getName() + " (tag=" + getTag() + ", value=" + getValue()
                + ")>";
    }
}
