/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.dlm.DataStreamLifecycleErrorStore.MAX_ERROR_MESSAGE_LENGTH;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleErrorStoreTests extends ESTestCase {

    private DataStreamLifecycleErrorStore errorStore;
    private ProjectId projectId;

    @Before
    public void setupServices() {
        errorStore = new DataStreamLifecycleErrorStore(System::currentTimeMillis);
        projectId = randomProjectIdOrDefault();
    }

    public void testRecordAndRetrieveError() {
        Index index = new Index("test", randomUUID());
        ErrorEntry existingRecordedError = errorStore.recordError(projectId, index, new NullPointerException("testing"));
        assertThat(existingRecordedError, is(nullValue()));
        assertThat(errorStore.getError(projectId, index), is(notNullValue()));
        assertThat(errorStore.getAllIndices(projectId).size(), is(1));
        assertThat(errorStore.getAllIndices(projectId), hasItem(index));

        existingRecordedError = errorStore.recordError(projectId, index, new IllegalStateException("bad state"));
        assertThat(existingRecordedError, is(notNullValue()));
        assertThat(existingRecordedError.error(), containsString("testing"));
    }

    public void testRetrieveAfterClear() {
        Index index = new Index("test", randomUUID());
        errorStore.recordError(projectId, index, new NullPointerException("testing"));
        errorStore.clearStore();
        assertThat(errorStore.getError(projectId, index), is(nullValue()));
    }

    public void testGetAllIndicesIsASnapshotViewOfTheStore() {
        List<Index> initialIndices = Stream.iterate(0, i -> i + 1).limit(5).map(i -> new Index("test" + i, randomUUID())).toList();
        initialIndices.forEach(index -> errorStore.recordError(projectId, index, new NullPointerException("testing")));
        Set<Index> initialErrorStoreIndices = errorStore.getAllIndices(projectId);
        assertThat(initialErrorStoreIndices.size(), is(5));
        assertThat(initialErrorStoreIndices, containsInAnyOrder(initialIndices.toArray(Index[]::new)));

        // let's add some more items to the store and clear a couple of the initial ones
        List<Index> moreIndices = Stream.iterate(5, i -> i + 1).limit(5).map(i -> new Index("test" + i, randomUUID())).toList();
        moreIndices.forEach(index -> errorStore.recordError(projectId, index, new NullPointerException("testing")));
        errorStore.clearRecordedError(projectId, initialIndices.get(0));
        errorStore.clearRecordedError(projectId, initialIndices.get(1));
        // the initial list should remain unchanged
        assertThat(initialErrorStoreIndices.size(), is(5));
        assertThat(initialErrorStoreIndices, containsInAnyOrder(initialIndices.toArray(Index[]::new)));

        // calling getAllIndices again should reflect the latest state
        Set<Index> recentErrorStoreIndices = errorStore.getAllIndices(projectId);
        assertThat(recentErrorStoreIndices.size(), is(8));
        List<Index> merged = new ArrayList<>(8);
        merged.addAll(initialIndices.subList(2, initialIndices.size()));
        merged.addAll(moreIndices);
        assertThat(recentErrorStoreIndices, containsInAnyOrder(merged.toArray(Index[]::new)));
    }

    public void testRecordedErrorIsMaxOneThousandChars() {
        Index index = new Index("test", randomUUID());
        NullPointerException exceptionWithLongMessage = new NullPointerException(randomAlphaOfLength(2000));
        errorStore.recordError(projectId, index, exceptionWithLongMessage);
        assertThat(errorStore.getError(projectId, index), is(notNullValue()));
        assertThat(errorStore.getError(projectId, index).error().length(), is(MAX_ERROR_MESSAGE_LENGTH));
    }

    public void testGetFilteredEntries() {
        Index index20 = new Index("test20", randomUUID());
        Index index5 = new Index("test5", randomUUID());
        IntStream.range(0, 20).forEach(i -> errorStore.recordError(projectId, index20, new NullPointerException("testing")));
        IntStream.range(0, 5).forEach(i -> errorStore.recordError(projectId, index5, new NullPointerException("testing")));

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 7, 100);
            assertThat(entries.size(), is(1));
            assertThat(entries.getFirst().indexName(), is("test20"));
            assertThat(entries.getFirst().projectId(), is(projectId));
        }

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 7, 0);
            assertThat(entries.size(), is(0));
        }

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 50, 100);
            assertThat(entries.size(), is(0));
        }

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 2, 100);
            assertThat(entries.size(), is(2));
            assertThat(entries.get(0).indexName(), is("test20"));
            assertThat(entries.get(0).projectId(), is(projectId));
            assertThat(entries.get(1).indexName(), is("test5"));
            assertThat(entries.get(1).projectId(), is(projectId));
        }
    }

    public void testGetFilteredEntriesForMultipleProjects() {
        ProjectId projectId1 = randomProjectIdOrDefault();
        ProjectId projectId2 = randomUniqueProjectId();
        Index index20 = new Index("test20", randomUUID());
        Index index5 = new Index("test5", randomUUID());
        IntStream.range(0, 20).forEach(i -> errorStore.recordError(projectId1, index20, new NullPointerException("testing")));
        IntStream.range(0, 5).forEach(i -> errorStore.recordError(projectId2, index5, new NullPointerException("testing")));

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 7, 100);
            assertThat(entries.size(), is(1));
            assertThat(entries.getFirst().indexName(), is("test20"));
            assertThat(entries.getFirst().projectId(), is(projectId1));
        }

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 7, 0);
            assertThat(entries.size(), is(0));
        }

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 50, 100);
            assertThat(entries.size(), is(0));
        }

        {
            List<DslErrorInfo> entries = errorStore.getErrorsInfo(entry -> entry.retryCount() > 2, 100);
            assertThat(entries.size(), is(2));
            assertThat(entries.get(0).indexName(), is("test20"));
            assertThat(entries.get(0).projectId(), is(projectId1));
            assertThat(entries.get(1).indexName(), is("test5"));
            assertThat(entries.get(1).projectId(), is(projectId2));
        }
    }

    public void testTotalErrorCount() {
        ProjectId projectId1 = randomProjectIdOrDefault();
        ProjectId projectId2 = randomUniqueProjectId();
        Index index1 = new Index("index1", randomUUID());
        Index index2 = new Index("index2", randomUUID());

        {
            // empty store
            assertThat(errorStore.getTotalErrorEntries(), is(0));
        }

        {
            // single project multiple indices
            IntStream.range(1, 20).forEach(i -> errorStore.recordError(projectId1, index1, new NullPointerException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId1, index2, new NullPointerException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId1, index2, new IOException("testing")));
            assertThat(errorStore.getTotalErrorEntries(), is(2));
        }

        {
            // clear store
            errorStore.clearStore();
            assertThat(errorStore.getTotalErrorEntries(), is(0));
        }

        {
            // multiple projects
            IntStream.range(1, 20).forEach(i -> errorStore.recordError(projectId1, index1, new NullPointerException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId1, index2, new IOException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId2, index1, new NullPointerException("testing")));
            assertThat(errorStore.getTotalErrorEntries(), is(3));
        }
    }
}
