/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestoreServiceTests extends ESTestCase {

    public void testUpdateDataStream() {
        String dataStreamName = "data-stream-1";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(backingIndexName))).thenReturn(indexMetadata);
        Index updatedIndex = new Index(backingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(updatedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest();

        DataStream updateDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(dataStreamName, updateDataStream.getName());
        assertEquals(Collections.singletonList(updatedIndex), updateDataStream.getIndices());
    }

    public void testUpdateDataStreamRename() {
        String dataStreamName = "data-stream-1";
        String renamedDataStreamName = "data-stream-2";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(indexMetadata);
        Index renamedIndex = new Index(renamedBackingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(renamedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().renamePattern("data-stream-1").renameReplacement("data-stream-2");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());
    }

    public void testPrefixNotChanged() {
        String dataStreamName = "ds-000001";
        String renamedDataStreamName = "ds2-000001";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(indexMetadata);
        Index renamedIndex = new Index(renamedBackingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(renamedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().renamePattern("ds-").renameReplacement("ds2-");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());

        request = new RestoreSnapshotRequest().renamePattern("ds-000001").renameReplacement("ds2-000001");

        renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());
    }
}
