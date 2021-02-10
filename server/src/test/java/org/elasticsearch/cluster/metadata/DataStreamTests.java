/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.DataStreamTestHelper;
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

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.containsString;
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
        DataStream rolledDs = ds.rollover(UUIDs.randomBase64UUID(), DataStream.NEW_FEATURES_VERSION);

        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getTimeStampField(), equalTo(ds.getTimeStampField()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
    }

    public void testRolloverWithLegacyBackingIndexNames() {
        DataStream ds = DataStreamTestHelper.randomInstance().promoteDataStream();
        DataStream rolledDs = ds.rollover(UUIDs.randomBase64UUID(), Version.V_7_10_0);

        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getTimeStampField(), equalTo(ds.getTimeStampField()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertThat(rolledDs.getWriteIndex().getName(),
            equalTo(DataStream.getLegacyDefaultBackingIndexName(ds.getName(), ds.getGeneration() + 1)));
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

    public void testReconcile() {
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

        boolean changedMetadata = randomBoolean();
        var postSnapshotMetadata = new HashMap<String, Object>();
        if (preSnapshotDataStream.getMetadata() != null) {
            postSnapshotMetadata.putAll(preSnapshotDataStream.getMetadata());
        }
        if (changedMetadata) {
            postSnapshotMetadata.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }

        var postSnapshotDataStream = new DataStream(
            preSnapshotDataStream.getName(),
            preSnapshotDataStream.getTimeStampField(),
            postSnapshotIndices,
            preSnapshotDataStream.getGeneration(),
            postSnapshotMetadata,
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated()
        );

        var reconciledDataStream = preSnapshotDataStream.reconcile(postSnapshotDataStream);

        assertThat(reconciledDataStream.getName(), equalTo(preSnapshotDataStream.getName()));
        assertThat(reconciledDataStream.getTimeStampField(), equalTo(preSnapshotDataStream.getTimeStampField()));
        assertThat(reconciledDataStream.getGeneration(), equalTo(preSnapshotDataStream.getGeneration()));
        assertThat(
            new HashSet<>(reconciledDataStream.getMetadata().entrySet()),
            hasItems(postSnapshotDataStream.getMetadata().entrySet().toArray())
        );
        assertThat(reconciledDataStream.isHidden(), equalTo(preSnapshotDataStream.isHidden()));
        assertThat(reconciledDataStream.isReplicated(), equalTo(preSnapshotDataStream.isReplicated()));
        assertThat(reconciledDataStream.getIndices(), everyItem(not(in(indicesToRemove))));
        assertThat(reconciledDataStream.getIndices(), everyItem(not(in(indicesToAdd))));
        assertThat(reconciledDataStream.getIndices().size(), equalTo(preSnapshotDataStream.getIndices().size() - indicesToRemove.size()));
    }

    public void testReconcileWithInvalidDifferences() {
        final String expectedError =
            "cannot reconcile data streams with differing names, timestamp fields, hidden statuses, or replicated statuses";
        var preSnapshotDataStream = DataStreamTestHelper.randomInstance();
        var changedName = new DataStream(
            randomAlphaOfLength(50),
            preSnapshotDataStream.getTimeStampField(),
            preSnapshotDataStream.getIndices(),
            preSnapshotDataStream.getGeneration(),
            preSnapshotDataStream.getMetadata(),
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated()
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> preSnapshotDataStream.reconcile(changedName));
        assertThat(e.getMessage(), containsString(expectedError));

        var changedHidden = new DataStream(
            preSnapshotDataStream.getName(),
            preSnapshotDataStream.getTimeStampField(),
            preSnapshotDataStream.getIndices(),
            preSnapshotDataStream.getGeneration(),
            preSnapshotDataStream.getMetadata(),
            preSnapshotDataStream.isHidden() ? false : true,
            preSnapshotDataStream.isReplicated()
        );
        e = expectThrows(IllegalArgumentException.class, () -> preSnapshotDataStream.reconcile(changedHidden));
        assertThat(e.getMessage(), containsString(expectedError));

        var changedReplicated = new DataStream(
            preSnapshotDataStream.getName(),
            preSnapshotDataStream.getTimeStampField(),
            preSnapshotDataStream.getIndices(),
            preSnapshotDataStream.getGeneration(),
            preSnapshotDataStream.getMetadata(),
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated() ? false : true
        );
        e = expectThrows(IllegalArgumentException.class, () -> preSnapshotDataStream.reconcile(changedReplicated));
        assertThat(e.getMessage(), containsString(expectedError));
    }

    public void testReconcileWithAllBackingIndicesRemoved() {
        var preSnapshotDataStream = DataStreamTestHelper.randomInstance();
        var indicesToAdd = DataStreamTestHelper.randomIndexInstances();

        var postSnapshotDataStream = new DataStream(
            preSnapshotDataStream.getName(),
            preSnapshotDataStream.getTimeStampField(),
            indicesToAdd,
            preSnapshotDataStream.getGeneration(),
            preSnapshotDataStream.getMetadata(),
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated()
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> preSnapshotDataStream.reconcile(postSnapshotDataStream)
        );
        assertThat(e.getMessage(), containsString("cannot reconcile data streams in which all pre-snapshot backing indices were deleted"));
    }

}
