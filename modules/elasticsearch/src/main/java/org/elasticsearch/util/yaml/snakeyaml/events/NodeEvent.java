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
package org.elasticsearch.util.yaml.snakeyaml.events;

import org.elasticsearch.util.yaml.snakeyaml.error.Mark;

/**
 * Base class for all events that mark the beginning of a node.
 */
public abstract class NodeEvent extends Event {

    private final String anchor;

    public NodeEvent(String anchor, Mark startMark, Mark endMark) {
        super(startMark, endMark);
        this.anchor = anchor;
    }

    /**
     * Node anchor by which this node might later be referenced by a
     * {@link AliasEvent}.
     * <p>
     * Note that {@link AliasEvent}s are by it self <code>NodeEvent</code>s and
     * use this property to indicate the referenced anchor.
     *
     * @return Anchor of this node or <code>null</code> if no anchor is defined.
     */
    public String getAnchor() {
        return this.anchor;
    }

    @Override
    protected String getArguments() {
        return "anchor=" + anchor;
    }
}
