/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class TransportResolveClusterActionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testHasNonClosedMatchingIndex() {
        List<ResolveIndexAction.ResolvedIndex> indices = Collections.emptyList();
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(false));

        // as long as there is one non-closed index it should return true
        indices = new ArrayList<>();
        indices.add(new ResolveIndexAction.ResolvedIndex("foo", null, new String[] { "open" }, ".ds-foo", null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("bar", null, new String[] { "system" }, ".ds-bar", null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("baz", null, new String[] { "system", "open", "hidden" }, null, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("quux", null, new String[0], null, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        indices.add(new ResolveIndexAction.ResolvedIndex("wibble", null, new String[] { "system", "closed" }, null, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));

        // if only closed indexes are present, should return false
        indices.clear();
        indices.add(new ResolveIndexAction.ResolvedIndex("wibble", null, new String[] { "system", "closed" }, null, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(false));
        indices.add(new ResolveIndexAction.ResolvedIndex("wobble", null, new String[] { "closed" }, null, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(false));

        // now add a non-closed index and should return true
        indices.add(new ResolveIndexAction.ResolvedIndex("aaa", null, new String[] { "hidden" }, null, null));
        assertThat(TransportResolveClusterAction.hasNonClosedMatchingIndex(indices), equalTo(true));
    }
}
