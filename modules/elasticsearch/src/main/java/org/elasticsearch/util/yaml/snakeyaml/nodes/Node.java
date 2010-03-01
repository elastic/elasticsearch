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

/**
 * Base class for all nodes.
 * <p>
 * The nodes form the node-graph described in the <a
 * href="http://yaml.org/spec/1.1/">YAML Specification</a>.
 * </p>
 * <p>
 * While loading, the node graph is usually created by the
 * {@link org.elasticsearch.util.yaml.snakeyaml.composer.Composer}, and later transformed into
 * application specific Java classes by the classes from the
 * {@link org.elasticsearch.util.yaml.snakeyaml.constructor} package.
 * </p>
 */
public abstract class Node {
    private Tag tag;
    private Mark startMark;
    protected Mark endMark;
    private Class<? extends Object> type;
    private boolean twoStepsConstruction;
    /**
     * true when the tag is assigned by the resolver
     */
    protected boolean resolved;
    protected Boolean useClassConstructor;

    public Node(Tag tag, Mark startMark, Mark endMark) {
        setTag(tag);
        this.startMark = startMark;
        this.endMark = endMark;
        this.type = Object.class;
        this.twoStepsConstruction = false;
        this.resolved = true;
        this.useClassConstructor = null;
    }

    /**
     * Tag of this node.
     * <p>
     * Every node has a tag assigned. The tag is either local or global.
     *
     * @return Tag of this node.
     */
    public Tag getTag() {
        return this.tag;
    }

    public Mark getEndMark() {
        return endMark;
    }

    /**
     * For error reporting.
     *
     * @return scalar, sequence, mapping
     * @see class variable 'id' in PyYAML
     */
    public abstract NodeId getNodeId();

    public Mark getStartMark() {
        return startMark;
    }

    public void setTag(Tag tag) {
        if (tag == null) {
            throw new NullPointerException("tag in a Node is required.");
        }
        this.tag = tag;
    }

    /*
     * It is not allowed to overwrite this method. Two Nodes are never equal.
     */

    @Override
    public final boolean equals(Object obj) {
        return super.equals(obj);
    }

    public Class<? extends Object> getType() {
        return type;
    }

    public void setType(Class<? extends Object> type) {
        this.type = type;
    }

    public void setTwoStepsConstruction(boolean twoStepsConstruction) {
        this.twoStepsConstruction = twoStepsConstruction;
    }

    /**
     * Indicates if this node must be constructed in two steps.
     * <p>
     * Two-step construction is required whenever a node is a child (direct or
     * indirect) of it self. That is, if a recursive structure is build using
     * anchors and aliases.
     * </p>
     * <p>
     * Set by {@link org.elasticsearch.util.yaml.snakeyaml.composer.Composer}, used during the
     * construction process.
     * </p>
     * <p>
     * Only relevant during loading.
     * </p>
     *
     * @return <code>true</code> if the node is self referenced.
     */
    public boolean isTwoStepsConstruction() {
        return twoStepsConstruction;
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    public boolean useClassConstructor() {
        if (useClassConstructor == null) {
            if (isResolved() && !Object.class.equals(type) && !tag.equals(Tag.NULL)) {
                return true;
            } else if (tag.isCompatible(getType())) {
                // the tag is compatible with the runtime class
                // the tag will be ignored
                return true;
            } else {
                return false;
            }
        }
        return useClassConstructor.booleanValue();
    }

    public void setUseClassConstructor(Boolean useClassConstructor) {
        this.useClassConstructor = useClassConstructor;
    }

    /**
     * Indicates if the tag was added by
     * {@link org.elasticsearch.util.yaml.snakeyaml.resolver.Resolver}.
     *
     * @return <code>true</code> if the tag of this node was resolved</code>
     */
    public boolean isResolved() {
        return resolved;
    }
}
