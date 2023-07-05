/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleErrorStoreTests extends ESTestCase {

    private DataStreamLifecycleErrorStore errorStore;

    @Before
    public void setupServices() {
        errorStore = new DataStreamLifecycleErrorStore();
    }

    public void testRecordAndRetrieveError() {
        errorStore.recordError("test", new NullPointerException("testing"));
        assertThat(errorStore.getError("test"), is(notNullValue()));
        assertThat(errorStore.getAllIndices().size(), is(1));
        assertThat(errorStore.getAllIndices().get(0), is("test"));
    }

    public void testRetrieveAfterClear() {
        errorStore.recordError("test", new NullPointerException("testing"));
        errorStore.clearStore();
        assertThat(errorStore.getError("test"), is(nullValue()));
    }

    public void testGetAllIndicesIsASnapshotViewOfTheStore() {
        Stream.iterate(0, i -> i + 1).limit(5).forEach(i -> errorStore.recordError("test" + i, new NullPointerException("testing")));
        List<String> initialAllIndices = errorStore.getAllIndices();
        assertThat(initialAllIndices.size(), is(5));
        assertThat(
            initialAllIndices,
            containsInAnyOrder(Stream.iterate(0, i -> i + 1).limit(5).map(i -> "test" + i).toArray(String[]::new))
        );

        // let's add some more items to the store and clear a couple of the initial ones
        Stream.iterate(5, i -> i + 1).limit(5).forEach(i -> errorStore.recordError("test" + i, new NullPointerException("testing")));
        errorStore.clearRecordedError("test0");
        errorStore.clearRecordedError("test1");
        // the initial list should remain unchanged
        assertThat(initialAllIndices.size(), is(5));
        assertThat(
            initialAllIndices,
            containsInAnyOrder(Stream.iterate(0, i -> i + 1).limit(5).map(i -> "test" + i).toArray(String[]::new))
        );

        // calling getAllIndices again should reflect the latest state
        assertThat(errorStore.getAllIndices().size(), is(8));
        assertThat(
            errorStore.getAllIndices(),
            containsInAnyOrder(Stream.iterate(2, i -> i + 1).limit(8).map(i -> "test" + i).toArray(String[]::new))
        );
    }
}
