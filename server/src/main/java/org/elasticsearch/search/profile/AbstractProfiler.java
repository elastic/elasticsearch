/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import java.util.List;

public class AbstractProfiler<PB extends AbstractProfileBreakdown<?>, E> {

    protected final AbstractInternalProfileTree<PB, E> profileTree;

    public AbstractProfiler(AbstractInternalProfileTree<PB, E> profileTree) {
        this.profileTree = profileTree;
    }

    /**
     * Get the {@link AbstractProfileBreakdown} for the given element in the
     * tree, potentially creating it if it did not exist.
     */
    public PB getQueryBreakdown(E query) {
        return profileTree.getProfileBreakdown(query);
    }

    /**
     * Removes the last (e.g. most recent) element on the stack.
     */
    public void pollLastElement() {
        profileTree.pollLast();
    }

    /**
     * @return a hierarchical representation of the profiled tree
     */
    public List<ProfileResult> getTree() {
        return profileTree.getTree();
    }

}
