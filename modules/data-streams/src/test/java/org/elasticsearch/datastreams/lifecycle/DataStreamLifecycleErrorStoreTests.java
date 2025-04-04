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
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore.MAX_ERROR_MESSAGE_LENGTH;
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
        ErrorEntry existingRecordedError = errorStore.recordError(projectId, "test", new NullPointerException("testing"));
        assertThat(existingRecordedError, is(nullValue()));
        assertThat(errorStore.getError(projectId, "test"), is(notNullValue()));
        assertThat(errorStore.getAllIndices(projectId).size(), is(1));
        assertThat(errorStore.getAllIndices(projectId), hasItem("test"));

        existingRecordedError = errorStore.recordError(projectId, "test", new IllegalStateException("bad state"));
        assertThat(existingRecordedError, is(notNullValue()));
        assertThat(existingRecordedError.error(), containsString("testing"));
    }

    public void testRetrieveAfterClear() {
        errorStore.recordError(projectId, "test", new NullPointerException("testing"));
        errorStore.clearStore();
        assertThat(errorStore.getError(projectId, "test"), is(nullValue()));
    }

    public void testGetAllIndicesIsASnapshotViewOfTheStore() {
        Stream.iterate(0, i -> i + 1)
            .limit(5)
            .forEach(i -> errorStore.recordError(projectId, "test" + i, new NullPointerException("testing")));
        Set<String> initialAllIndices = errorStore.getAllIndices(projectId);
        assertThat(initialAllIndices.size(), is(5));
        assertThat(
            initialAllIndices,
            containsInAnyOrder(Stream.iterate(0, i -> i + 1).limit(5).map(i -> "test" + i).toArray(String[]::new))
        );

        // let's add some more items to the store and clear a couple of the initial ones
        Stream.iterate(5, i -> i + 1)
            .limit(5)
            .forEach(i -> errorStore.recordError(projectId, "test" + i, new NullPointerException("testing")));
        errorStore.clearRecordedError(projectId, "test0");
        errorStore.clearRecordedError(projectId, "test1");
        // the initial list should remain unchanged
        assertThat(initialAllIndices.size(), is(5));
        assertThat(
            initialAllIndices,
            containsInAnyOrder(Stream.iterate(0, i -> i + 1).limit(5).map(i -> "test" + i).toArray(String[]::new))
        );

        // calling getAllIndices again should reflect the latest state
        assertThat(errorStore.getAllIndices(projectId).size(), is(8));
        assertThat(
            errorStore.getAllIndices(projectId),
            containsInAnyOrder(Stream.iterate(2, i -> i + 1).limit(8).map(i -> "test" + i).toArray(String[]::new))
        );
    }

    public void testRecordedErrorIsMaxOneThousandChars() {
        NullPointerException exceptionWithLongMessage = new NullPointerException(randomAlphaOfLength(2000));
        errorStore.recordError(projectId, "test", exceptionWithLongMessage);
        assertThat(errorStore.getError(projectId, "test"), is(notNullValue()));
        assertThat(errorStore.getError(projectId, "test").error().length(), is(MAX_ERROR_MESSAGE_LENGTH));
    }

    public void testGetFilteredEntries() {
        IntStream.range(0, 20).forEach(i -> errorStore.recordError(projectId, "test20", new NullPointerException("testing")));
        IntStream.range(0, 5).forEach(i -> errorStore.recordError(projectId, "test5", new NullPointerException("testing")));

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
        IntStream.range(0, 20).forEach(i -> errorStore.recordError(projectId1, "test20", new NullPointerException("testing")));
        IntStream.range(0, 5).forEach(i -> errorStore.recordError(projectId2, "test5", new NullPointerException("testing")));

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

        {
            // empty store
            assertThat(errorStore.getTotalErrorEntries(), is(0));
        }

        {
            // single project multiple indices
            IntStream.range(1, 20).forEach(i -> errorStore.recordError(projectId1, "index1", new NullPointerException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId1, "index2", new NullPointerException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId1, "index2", new IOException("testing")));
            assertThat(errorStore.getTotalErrorEntries(), is(2));
        }

        {
            // clear store
            errorStore.clearStore();
            assertThat(errorStore.getTotalErrorEntries(), is(0));
        }

        {
            // multiple projects
            IntStream.range(1, 20).forEach(i -> errorStore.recordError(projectId1, "index1", new NullPointerException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId1, "index2", new IOException("testing")));
            IntStream.range(1, 5).forEach(i -> errorStore.recordError(projectId2, "index1", new NullPointerException("testing")));
            assertThat(errorStore.getTotalErrorEntries(), is(3));
        }
    }
}
