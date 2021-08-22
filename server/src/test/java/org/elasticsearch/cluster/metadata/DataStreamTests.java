/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;

public class DataStreamTests extends AbstractSerializingTestCase<DataStream> {

    @Override
    protected DataStream doParseInstance(XContentParser parser) throws IOException {
        return DataStream.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStream> instanceReader() {
        return DataStream::new;
    }

    @Override
    protected DataStream createTestInstance() {
        return DataStreamTestHelper.randomInstance();
    }

    public void testRollover() {
        DataStream ds = DataStreamTestHelper.randomInstance().promoteDataStream();
        DataStream rolledDs = ds.rollover(Metadata.EMPTY_METADATA, UUIDs.randomBase64UUID());

        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getTimeStampField(), equalTo(ds.getTimeStampField()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
    }

    public void testRolloverWithConflictingBackingIndexName() {
        // used a fixed time provider to guarantee name conflicts
        DataStream ds = DataStreamTestHelper.randomInstance(() -> 0L).promoteDataStream();

        // create some indices with names that conflict with the names of the data stream's backing indices
        int numConflictingIndices = randomIntBetween(1, 10);
        Metadata.Builder builder = Metadata.builder();
        for (int k = 1; k <= numConflictingIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(ds.getName(), ds.getGeneration() + k, 0L))
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }

        DataStream rolledDs = ds.rollover(builder.build(), UUIDs.randomBase64UUID());
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getTimeStampField(), equalTo(ds.getTimeStampField()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + numConflictingIndices + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
    }

    public void testRemoveBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int indexToRemove = randomIntBetween(1, numBackingIndices - 1);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);
        DataStream updated = original.removeBackingIndex(indices.get(indexToRemove - 1));
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration()));
        assertThat(updated.getTimeStampField(), equalTo(original.getTimeStampField()));
        assertThat(updated.getIndices().size(), equalTo(numBackingIndices - 1));
        for (int k = 0; k < (numBackingIndices - 1); k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k < (indexToRemove - 1) ? k : k + 1)));
        }
    }

    public void testDefaultBackingIndexName() {
        // this test does little more than flag that changing the default naming convention for backing indices
        // will also require changing a lot of hard-coded values in REST tests and docs
        long backingIndexNum = randomLongBetween(1, 1000001);
        String dataStreamName = randomAlphaOfLength(6);
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        String dateString = DataStream.DATE_FORMATTER.formatMillis(epochMillis);
        String defaultBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNum, epochMillis);
        String expectedBackingIndexName = String.format(Locale.ROOT, ".ds-%s-%s-%06d", dataStreamName, dateString, backingIndexNum);
        assertThat(defaultBackingIndexName, equalTo(expectedBackingIndexName));
    }

    public void testReplaceBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int indexToReplace = randomIntBetween(1, numBackingIndices - 1) - 1;
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int i = 1; i <= numBackingIndices; i++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        DataStream updated = original.replaceBackingIndex(indices.get(indexToReplace), newBackingIndex);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration()));
        assertThat(updated.getTimeStampField(), equalTo(original.getTimeStampField()));
        assertThat(updated.getIndices().size(), equalTo(numBackingIndices));
        assertThat(updated.getIndices().get(indexToReplace), equalTo(newBackingIndex));

        for (int i = 0; i < numBackingIndices; i++) {
            if (i != indexToReplace) {
                assertThat(updated.getIndices().get(i), equalTo(original.getIndices().get(i)));
            }
        }
    }

    public void testReplaceBackingIndexThrowsExceptionIfIndexNotPartOfDataStream() {
        int numBackingIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int i = 1; i <= numBackingIndices; i++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Index standaloneIndex = new Index("index-foo", UUIDs.randomBase64UUID(random()));
        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceBackingIndex(standaloneIndex, newBackingIndex));
    }

    public void testReplaceBackingIndexThrowsExceptionIfReplacingWriteIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int writeIndexPosition = numBackingIndices - 1;
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int i = 1; i <= numBackingIndices; i++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceBackingIndex(indices.get(writeIndexPosition), newBackingIndex));
    }

    public void testSnapshot() {
        var preSnapshotDataStream = DataStreamTestHelper.randomInstance();
        var indicesToRemove = randomSubsetOf(preSnapshotDataStream.getIndices());
        if (indicesToRemove.size() == preSnapshotDataStream.getIndices().size()) {
            // never remove them all
            indicesToRemove.remove(0);
        }
        var indicesToAdd = DataStreamTestHelper.randomIndexInstances();
        var postSnapshotIndices = new ArrayList<>(preSnapshotDataStream.getIndices());
        postSnapshotIndices.removeAll(indicesToRemove);
        postSnapshotIndices.addAll(indicesToAdd);

        var postSnapshotDataStream = new DataStream(
            preSnapshotDataStream.getName(),
            preSnapshotDataStream.getTimeStampField(),
            postSnapshotIndices,
            preSnapshotDataStream.getGeneration() + randomIntBetween(0, 5),
            preSnapshotDataStream.getMetadata() == null ? null : new HashMap<>(preSnapshotDataStream.getMetadata()),
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated() && randomBoolean(), preSnapshotDataStream.isAllowCustomRouting());

        var reconciledDataStream =
            postSnapshotDataStream.snapshot(preSnapshotDataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList()));

        assertThat(reconciledDataStream.getName(), equalTo(postSnapshotDataStream.getName()));
        assertThat(reconciledDataStream.getTimeStampField(), equalTo(postSnapshotDataStream.getTimeStampField()));
        assertThat(reconciledDataStream.getGeneration(), equalTo(postSnapshotDataStream.getGeneration()));
        if (reconciledDataStream.getMetadata() != null) {
            assertThat(
                new HashSet<>(reconciledDataStream.getMetadata().entrySet()),
                hasItems(postSnapshotDataStream.getMetadata().entrySet().toArray())
            );
        } else {
            assertNull(postSnapshotDataStream.getMetadata());
        }
        assertThat(reconciledDataStream.isHidden(), equalTo(postSnapshotDataStream.isHidden()));
        assertThat(reconciledDataStream.isReplicated(), equalTo(postSnapshotDataStream.isReplicated()));
        assertThat(reconciledDataStream.getIndices(), everyItem(not(in(indicesToRemove))));
        assertThat(reconciledDataStream.getIndices(), everyItem(not(in(indicesToAdd))));
        assertThat(reconciledDataStream.getIndices().size(), equalTo(preSnapshotDataStream.getIndices().size() - indicesToRemove.size()));
    }

    public void testSnapshotWithAllBackingIndicesRemoved() {
        var preSnapshotDataStream = DataStreamTestHelper.randomInstance();
        var indicesToAdd = new ArrayList<Index>();
        while (indicesToAdd.isEmpty()) {
            // ensure at least one index
            indicesToAdd.addAll(DataStreamTestHelper.randomIndexInstances());
        }

        var postSnapshotDataStream = new DataStream(
            preSnapshotDataStream.getName(),
            preSnapshotDataStream.getTimeStampField(),
            indicesToAdd,
            preSnapshotDataStream.getGeneration(),
            preSnapshotDataStream.getMetadata(),
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated(),
            preSnapshotDataStream.isAllowCustomRouting()
        );

        assertNull(postSnapshotDataStream.snapshot(
                preSnapshotDataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList())));
    }
}
