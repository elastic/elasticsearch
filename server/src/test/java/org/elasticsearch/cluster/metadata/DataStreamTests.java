/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.randomGlobalRetention;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.randomIndexInstances;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.randomNonEmptyIndexInstances;
import static org.elasticsearch.index.IndexSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamTests extends AbstractXContentSerializingTestCase<DataStream> {

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

    @Override
    protected DataStream mutateInstance(DataStream instance) {
        var name = instance.getName();
        var indices = instance.getIndices();
        var generation = instance.getGeneration();
        var metadata = instance.getMetadata();
        var isHidden = instance.isHidden();
        var isReplicated = instance.isReplicated();
        var isSystem = instance.isSystem();
        var allowsCustomRouting = instance.isAllowCustomRouting();
        var indexMode = instance.getIndexMode();
        var lifecycle = instance.getLifecycle();
        var failureStore = instance.isFailureStore();
        var failureIndices = instance.getFailureIndices();
        var autoShardingEvent = instance.getAutoShardingEvent();
        switch (between(0, 11)) {
            case 0 -> name = randomAlphaOfLength(10);
            case 1 -> indices = randomNonEmptyIndexInstances();
            case 2 -> generation = instance.getGeneration() + randomIntBetween(1, 10);
            case 3 -> metadata = randomBoolean() && metadata != null ? null : Map.of("key", randomAlphaOfLength(10));
            case 4 -> {
                if (isHidden) {
                    isHidden = false;
                    isSystem = false; // To ensure that we generate a valid combination, system indices should be always hidden
                } else {
                    isHidden = true;
                }
            }
            case 5 -> isReplicated = isReplicated == false;
            case 6 -> {
                if (isSystem == false) {
                    isSystem = true;
                    isHidden = true; // A system data stream must always be hidden
                } else {
                    isSystem = false;
                }
            }
            case 7 -> allowsCustomRouting = allowsCustomRouting == false;
            case 8 -> indexMode = randomBoolean() && indexMode != null
                ? null
                : randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 9 -> lifecycle = randomBoolean() && lifecycle != null
                ? null
                : DataStreamLifecycle.newBuilder().dataRetention(randomMillisUpToYear9999()).build();
            case 10 -> {
                failureIndices = randomValueOtherThan(failureIndices, DataStreamTestHelper::randomIndexInstances);
                failureStore = failureIndices.isEmpty() == false;
            }
            case 11 -> {
                autoShardingEvent = randomBoolean() && autoShardingEvent != null
                    ? null
                    : new DataStreamAutoShardingEvent(
                        indices.get(indices.size() - 1).getName(),
                        randomIntBetween(1, 10),
                        randomMillisUpToYear9999()
                    );
            }
        }

        return new DataStream(
            name,
            indices,
            generation,
            metadata,
            isHidden,
            isReplicated,
            isSystem,
            allowsCustomRouting,
            indexMode,
            lifecycle,
            failureStore,
            failureIndices,
            autoShardingEvent
        );
    }

    public void testRollover() {
        DataStream ds = DataStreamTestHelper.randomInstance().promoteDataStream();
        Tuple<String, Long> newCoordinates = ds.nextWriteIndexAndGeneration(Metadata.EMPTY_METADATA);
        final DataStream rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), false, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
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
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }

        final Tuple<String, Long> newCoordinates = ds.nextWriteIndexAndGeneration(builder.build());
        final DataStream rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), false, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + numConflictingIndices + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), equalTo(ds.getIndexMode()));
    }

    public void testRolloverUpgradeToTsdbDataStream() {
        IndexMode indexMode = randomBoolean() ? IndexMode.STANDARD : null;
        DataStream ds = DataStreamTestHelper.randomInstance().promoteDataStream();
        // Unsure index_mode=null
        ds = new DataStream(
            ds.getName(),
            ds.getIndices(),
            ds.getGeneration(),
            ds.getMetadata(),
            ds.isHidden(),
            ds.isReplicated(),
            ds.isSystem(),
            ds.isAllowCustomRouting(),
            indexMode,
            ds.getLifecycle(),
            ds.isFailureStore(),
            ds.getFailureIndices(),
            ds.getAutoShardingEvent()
        );
        var newCoordinates = ds.nextWriteIndexAndGeneration(Metadata.EMPTY_METADATA);

        var rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), true, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), equalTo(IndexMode.TIME_SERIES));
    }

    public void testRolloverDowngradeToRegularDataStream() {
        DataStream ds = DataStreamTestHelper.randomInstance().promoteDataStream();
        ds = new DataStream(
            ds.getName(),
            ds.getIndices(),
            ds.getGeneration(),
            ds.getMetadata(),
            ds.isHidden(),
            ds.isReplicated(),
            ds.isSystem(),
            ds.isAllowCustomRouting(),
            IndexMode.TIME_SERIES,
            ds.getLifecycle(),
            ds.isFailureStore(),
            ds.getFailureIndices(),
            ds.getAutoShardingEvent()
        );
        var newCoordinates = ds.nextWriteIndexAndGeneration(Metadata.EMPTY_METADATA);

        var rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), false, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), nullValue());
    }

    public void testRolloverFailureStore() {
        DataStream ds = DataStreamTestHelper.randomInstance(true).promoteDataStream();
        Tuple<String, Long> newCoordinates = ds.nextFailureStoreWriteIndexAndGeneration(Metadata.EMPTY_METADATA);
        final DataStream rolledDs = ds.rolloverFailureStore(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2());
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size()));
        assertThat(rolledDs.getFailureIndices().size(), equalTo(ds.getFailureIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertTrue(rolledDs.getFailureIndices().containsAll(ds.getFailureIndices()));
        assertTrue(rolledDs.getFailureIndices().contains(rolledDs.getFailureStoreWriteIndex()));
    }

    public void testRemoveBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int indexToRemove = randomIntBetween(1, numBackingIndices - 1);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);
        DataStream updated = original.removeBackingIndex(indices.get(indexToRemove - 1));
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(numBackingIndices - 1));
        for (int k = 0; k < (numBackingIndices - 1); k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k < (indexToRemove - 1) ? k : k + 1)));
        }
    }

    public void testRemoveBackingIndexThatDoesNotExist() {
        int numBackingIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);

        final Index indexToRemove = new Index(randomAlphaOfLength(4), UUIDs.randomBase64UUID(random()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> original.removeBackingIndex(indexToRemove));
        assertThat(
            e.getMessage(),
            equalTo(Strings.format("index [%s] is not part of data stream [%s]", indexToRemove.getName(), dataStreamName))
        );
    }

    public void testRemoveBackingWriteIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> original.removeBackingIndex(indices.get(numBackingIndices - 1))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot remove backing index [%s] of data stream [%s] because it is the write index",
                    indices.get(numBackingIndices - 1).getName(),
                    dataStreamName
                )
            )
        );
    }

    public void testAddBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final long epochMillis = System.currentTimeMillis();

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k, epochMillis), UUIDs.randomBase64UUID(random())));
        }

        Metadata.Builder builder = Metadata.builder();
        for (int k = 1; k <= numBackingIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(indices.get(k - 1).getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);
        builder.put(original);
        Index indexToAdd = new Index(randomAlphaOfLength(4), UUIDs.randomBase64UUID(random()));
        builder.put(
            IndexMetadata.builder(indexToAdd.getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(),
            false
        );

        DataStream updated = original.addBackingIndex(builder.build(), indexToAdd);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(numBackingIndices + 1));
        for (int k = 1; k <= numBackingIndices; k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k - 1)));
        }
        assertThat(updated.getIndices().get(0), equalTo(indexToAdd));
    }

    public void testAddBackingIndexThatIsPartOfAnotherDataStream() {
        int numBackingIndices = randomIntBetween(2, 32);
        final String dsName1 = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String dsName2 = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final long epochMillis = System.currentTimeMillis();

        List<Index> indices1 = new ArrayList<>(numBackingIndices);
        List<Index> indices2 = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices1.add(new Index(DataStream.getDefaultBackingIndexName(dsName1, k, epochMillis), UUIDs.randomBase64UUID(random())));
            indices2.add(new Index(DataStream.getDefaultBackingIndexName(dsName2, k, epochMillis), UUIDs.randomBase64UUID(random())));
        }

        Metadata.Builder builder = Metadata.builder();
        for (int k = 1; k <= numBackingIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(indices1.get(k - 1).getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
            im = IndexMetadata.builder(indices2.get(k - 1).getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }
        DataStream ds1 = DataStreamTestHelper.newInstance(dsName1, indices1);
        DataStream ds2 = DataStreamTestHelper.newInstance(dsName2, indices2);
        builder.put(ds1);
        builder.put(ds2);

        Index indexToAdd = randomFrom(indices2.toArray(Index.EMPTY_ARRAY));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ds1.addBackingIndex(builder.build(), indexToAdd));
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] because it is already a backing index on data stream [%s]",
                    indexToAdd.getName(),
                    ds1.getName(),
                    ds2.getName()
                )
            )
        );
    }

    public void testAddExistingBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final long epochMillis = System.currentTimeMillis();

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k, epochMillis), UUIDs.randomBase64UUID(random())));
        }

        Metadata.Builder builder = Metadata.builder();
        for (int k = 1; k <= numBackingIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(indices.get(k - 1).getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);
        builder.put(original);
        Index indexToAdd = randomFrom(indices.toArray(Index.EMPTY_ARRAY));

        DataStream updated = original.addBackingIndex(builder.build(), indexToAdd);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration()));
        assertThat(updated.getIndices().size(), equalTo(numBackingIndices));
        for (int k = 0; k < numBackingIndices; k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k)));
        }
    }

    public void testAddBackingIndexWithAliases() {
        int numBackingIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final long epochMillis = System.currentTimeMillis();

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k, epochMillis), UUIDs.randomBase64UUID(random())));
        }

        Metadata.Builder builder = Metadata.builder();
        for (int k = 1; k <= numBackingIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(indices.get(k - 1).getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);
        builder.put(original);

        Index indexToAdd = new Index(randomAlphaOfLength(4), UUIDs.randomBase64UUID(random()));
        IndexMetadata.Builder b = IndexMetadata.builder(indexToAdd.getName())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final int numAliases = randomIntBetween(1, 3);
        final String[] aliasNames = new String[numAliases];
        for (int k = 0; k < numAliases; k++) {
            aliasNames[k] = randomAlphaOfLength(6);
            b.putAlias(AliasMetadata.builder(aliasNames[k]));
        }
        builder.put(b.build(), false);
        Arrays.sort(aliasNames);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> original.addBackingIndex(builder.build(), indexToAdd)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] until its alias(es) [%s] are removed",
                    indexToAdd.getName(),
                    original.getName(),
                    Strings.arrayToCommaDelimitedString(aliasNames)
                )
            )
        );
    }

    public void testDefaultBackingIndexName() {
        // this test does little more than flag that changing the default naming convention for backing indices
        // will also require changing a lot of hard-coded values in REST tests and docs
        long backingIndexNum = randomLongBetween(1, 1000001);
        String dataStreamName = randomAlphaOfLength(6);
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        String dateString = DataStream.DATE_FORMATTER.formatMillis(epochMillis);
        String defaultBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNum, epochMillis);
        String expectedBackingIndexName = Strings.format(".ds-%s-%s-%06d", dataStreamName, dateString, backingIndexNum);
        assertThat(defaultBackingIndexName, equalTo(expectedBackingIndexName));
    }

    public void testDefaultFailureStoreName() {
        // this test does little more than flag that changing the default naming convention for failure store indices
        // will also require changing a lot of hard-coded values in REST tests and docs
        long failureStoreIndexNum = randomLongBetween(1, 1000001);
        String dataStreamName = randomAlphaOfLength(6);
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        String dateString = DataStream.DATE_FORMATTER.formatMillis(epochMillis);
        String defaultFailureStoreName = DataStream.getDefaultFailureStoreName(dataStreamName, failureStoreIndexNum, epochMillis);
        String expectedFailureStoreName = Strings.format(".fs-%s-%s-%06d", dataStreamName, dateString, failureStoreIndexNum);
        assertThat(defaultFailureStoreName, equalTo(expectedFailureStoreName));
    }

    public void testReplaceBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int indexToReplace = randomIntBetween(1, numBackingIndices - 1) - 1;
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int i = 1; i <= numBackingIndices; i++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        DataStream updated = original.replaceBackingIndex(indices.get(indexToReplace), newBackingIndex);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
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
        DataStream original = DataStreamTestHelper.newInstance(dataStreamName, indices);

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
        int generation = randomBoolean() ? numBackingIndices : numBackingIndices + randomIntBetween(1, 5);
        DataStream original = newInstance(dataStreamName, indices, generation, null);

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> original.replaceBackingIndex(indices.get(writeIndexPosition), newBackingIndex)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot replace backing index [%s] of data stream [%s] because it is the write index",
                    indices.get(writeIndexPosition).getName(),
                    dataStreamName
                )
            )
        );
    }

    public void testSnapshot() {
        var preSnapshotDataStream = DataStreamTestHelper.randomInstance();
        var indicesToRemove = randomSubsetOf(preSnapshotDataStream.getIndices());
        if (indicesToRemove.size() == preSnapshotDataStream.getIndices().size()) {
            // never remove them all
            indicesToRemove.remove(0);
        }
        var indicesToAdd = randomIndexInstances();
        var postSnapshotIndices = new ArrayList<>(preSnapshotDataStream.getIndices());
        postSnapshotIndices.removeAll(indicesToRemove);
        postSnapshotIndices.addAll(indicesToAdd);

        var postSnapshotDataStream = new DataStream(
            preSnapshotDataStream.getName(),
            postSnapshotIndices,
            preSnapshotDataStream.getGeneration() + randomIntBetween(0, 5),
            preSnapshotDataStream.getMetadata() == null ? null : new HashMap<>(preSnapshotDataStream.getMetadata()),
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated() && randomBoolean(),
            preSnapshotDataStream.isSystem(),
            preSnapshotDataStream.isAllowCustomRouting(),
            preSnapshotDataStream.getIndexMode(),
            preSnapshotDataStream.getLifecycle(),
            preSnapshotDataStream.isFailureStore(),
            preSnapshotDataStream.getFailureIndices(),
            preSnapshotDataStream.getAutoShardingEvent()
        );

        var reconciledDataStream = postSnapshotDataStream.snapshot(
            preSnapshotDataStream.getIndices().stream().map(Index::getName).toList()
        );

        assertThat(reconciledDataStream.getName(), equalTo(postSnapshotDataStream.getName()));
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
        var indicesToAdd = randomNonEmptyIndexInstances();

        var postSnapshotDataStream = new DataStream(
            preSnapshotDataStream.getName(),
            indicesToAdd,
            preSnapshotDataStream.getGeneration(),
            preSnapshotDataStream.getMetadata(),
            preSnapshotDataStream.isHidden(),
            preSnapshotDataStream.isReplicated(),
            preSnapshotDataStream.isSystem(),
            preSnapshotDataStream.isAllowCustomRouting(),
            preSnapshotDataStream.getIndexMode(),
            preSnapshotDataStream.getLifecycle(),
            preSnapshotDataStream.isFailureStore(),
            preSnapshotDataStream.getFailureIndices(),
            preSnapshotDataStream.getAutoShardingEvent()
        );

        assertNull(postSnapshotDataStream.snapshot(preSnapshotDataStream.getIndices().stream().map(Index::getName).toList()));
    }

    public void testSelectTimeSeriesWriteIndex() {
        Instant currentTime = Instant.now();

        Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
        Instant end1 = currentTime.minus(2, ChronoUnit.HOURS);
        Instant start2 = currentTime.minus(2, ChronoUnit.HOURS);
        Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);

        String dataStreamName = "logs_my-app_prod";
        ClusterState clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2))
        );

        DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);
        Index result = dataStream.selectTimeSeriesWriteIndex(currentTime, clusterState.getMetadata());
        assertThat(result, equalTo(dataStream.getIndices().get(1)));
        assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, start2.toEpochMilli())));

        result = dataStream.selectTimeSeriesWriteIndex(currentTime.minus(2, ChronoUnit.HOURS), clusterState.getMetadata());
        assertThat(result, equalTo(dataStream.getIndices().get(1)));
        assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, start2.toEpochMilli())));

        result = dataStream.selectTimeSeriesWriteIndex(currentTime.minus(3, ChronoUnit.HOURS), clusterState.getMetadata());
        assertThat(result, equalTo(dataStream.getIndices().get(0)));
        assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, start1.toEpochMilli())));

        result = dataStream.selectTimeSeriesWriteIndex(currentTime.minus(6, ChronoUnit.HOURS), clusterState.getMetadata());
        assertThat(result, equalTo(dataStream.getIndices().get(0)));
        assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, start1.toEpochMilli())));
    }

    public void testValidate() {
        {
            // Valid cases:
            Instant currentTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            // These ranges are on the edge of each other temporal boundaries.
            Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
            Instant end1 = currentTime.minus(2, ChronoUnit.HOURS);
            Instant start2 = currentTime.minus(2, ChronoUnit.HOURS);
            Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);

            String dataStreamName = "logs_my-app_prod";
            var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
                dataStreamName,
                List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2))
            );
            DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);
            assertThat(dataStream, notNullValue());
            assertThat(dataStream.getIndices(), hasSize(2));
            assertThat(
                IndexSettings.TIME_SERIES_START_TIME.get(clusterState.getMetadata().index(dataStream.getIndices().get(0)).getSettings()),
                equalTo(start1)
            );
            assertThat(
                IndexSettings.TIME_SERIES_END_TIME.get(clusterState.getMetadata().index(dataStream.getIndices().get(0)).getSettings()),
                equalTo(end1)
            );
            assertThat(
                IndexSettings.TIME_SERIES_START_TIME.get(clusterState.getMetadata().index(dataStream.getIndices().get(1)).getSettings()),
                equalTo(start2)
            );
            assertThat(
                IndexSettings.TIME_SERIES_END_TIME.get(clusterState.getMetadata().index(dataStream.getIndices().get(1)).getSettings()),
                equalTo(end2)
            );

            // Create a temporal gap between, this is valid and shouldn't fail:
            DataStreamTestHelper.getClusterStateWithDataStream(
                dataStreamName,
                List.of(Tuple.tuple(start1, end1.minus(1, ChronoUnit.MINUTES)), Tuple.tuple(start2.plus(1, ChronoUnit.MINUTES), end2))
            );
        }
        {
            // Invalid case:
            Instant currentTime = Instant.now();

            Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
            Instant end1 = currentTime.minus(2, ChronoUnit.HOURS);
            // Start2 is inside start1 and end1 range:
            Instant start2 = currentTime.minus(3, ChronoUnit.HOURS);
            Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);

            String dataStreamName = "logs_my-app_prod";
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamTestHelper.getClusterStateWithDataStream(
                    dataStreamName,
                    List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2))
                )
            );
            var formatter = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
            assertThat(
                e.getMessage(),
                equalTo(
                    "backing index ["
                        + DataStream.getDefaultBackingIndexName(dataStreamName, 1, start1.toEpochMilli())
                        + "] with range ["
                        + formatter.format(start1)
                        + " TO "
                        + formatter.format(end1)
                        + "] is overlapping with backing index ["
                        + DataStream.getDefaultBackingIndexName(dataStreamName, 2, start2.toEpochMilli())
                        + "] with range ["
                        + formatter.format(start2)
                        + " TO "
                        + formatter.format(end2)
                        + "]"
                )
            );
        }
        {
            Instant currentTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            // These ranges are on the edge of each other temporal boundaries.
            Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
            Instant end1 = currentTime.minus(2, ChronoUnit.HOURS);
            Instant start2 = currentTime.minus(2, ChronoUnit.HOURS);
            Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);

            String dataStreamName = "logs_my-app_prod";
            var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
                dataStreamName,
                List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2))
            );
            DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);

            {
                // IndexMetadata not found case:
                var e = expectThrows(IllegalStateException.class, () -> dataStream.validate((index) -> null));
                assertThat(
                    e.getMessage(),
                    equalTo(
                        "index ["
                            + DataStream.getDefaultBackingIndexName(dataStreamName, 1, start1.toEpochMilli())
                            + "] is not found in the index metadata supplier"
                    )
                );
            }

            {
                // index is not time_series index:
                dataStream.validate(
                    (index) -> IndexMetadata.builder(index)
                        .settings(
                            Settings.builder()
                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
                                .build()
                        )
                        .build()
                );
            }

            {
                // invalid IndexMetadata result
                Instant start3 = currentTime.minus(6, ChronoUnit.HOURS);
                Instant end3 = currentTime.plus(2, ChronoUnit.HOURS);
                var e = expectThrows(
                    IllegalArgumentException.class,
                    () -> dataStream.validate(
                        (index) -> IndexMetadata.builder(index)
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
                                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), start3.toEpochMilli())
                                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), end3.toEpochMilli())
                                    .build()
                            )
                            .build()
                    )
                );
                var formatter = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
                assertThat(
                    e.getMessage(),
                    equalTo(
                        "backing index ["
                            + DataStream.getDefaultBackingIndexName(dataStreamName, 1, start1.toEpochMilli())
                            + "] with range ["
                            + formatter.format(start3)
                            + " TO "
                            + formatter.format(end3)
                            + "] is overlapping with backing index ["
                            + DataStream.getDefaultBackingIndexName(dataStreamName, 2, start2.toEpochMilli())
                            + "] with range ["
                            + formatter.format(start3)
                            + " TO "
                            + formatter.format(end3)
                            + "]"
                    )
                );
            }
        }
    }

    public void testGetGenerationLifecycleDate() {
        String dataStreamName = "metrics-foo";
        long now = System.currentTimeMillis();
        long creationTimeMillis = now - 3000L;
        long rolloverTimeMills = now - 2000L;

        {
            // for the write index we get the null
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            DataStream dataStream = new DataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex()),
                1L,
                Map.of(),
                false,
                randomBoolean(),
                false,
                randomBoolean(),
                IndexMode.STANDARD
            );
            assertNull(dataStream.getGenerationLifecycleDate(indexMetadata));
        }

        {
            // for rolled indices we get the rollover info for the specified data stream
            IndexMetadata.Builder writeIndexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);

            MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(rolloverTimeMills));
            indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            DataStream dataStream = new DataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex()),
                1L,
                Map.of(),
                false,
                randomBoolean(),
                false,
                randomBoolean(),
                IndexMode.STANDARD
            );
            assertThat(dataStream.getGenerationLifecycleDate(indexMetadata).millis(), is(rolloverTimeMills));
        }

        {
            // for rolled indices on other targets than the data stream name we get the creation date
            IndexMetadata.Builder writeIndexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);

            MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(rolloverTimeMills));
            indexMetaBuilder.putRolloverInfo(new RolloverInfo("some-alias-name", List.of(rolloverCondition), now - 2000L));
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            DataStream dataStream = new DataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex()),
                1L,
                Map.of(),
                false,
                randomBoolean(),
                false,
                randomBoolean(),
                IndexMode.STANDARD
            );
            assertThat(dataStream.getGenerationLifecycleDate(indexMetadata).millis(), is(creationTimeMillis));
        }
        {
            // for a write index that has not been rolled over yet, we get null even if the index has an origination date
            long originTimeMillis = creationTimeMillis - 3000L;
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .settings(settings(IndexVersion.current()).put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            DataStream dataStream = new DataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex()),
                1L,
                Map.of(),
                false,
                randomBoolean(),
                false,
                randomBoolean(),
                IndexMode.STANDARD
            );

            assertNull(dataStream.getGenerationLifecycleDate(indexMetadata));
        }
        {
            // If the index is not the write index and has origination date set, we get the origination date even if it has not been
            // rolled over
            IndexMetadata.Builder writeIndexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);
            long originTimeMillis = creationTimeMillis - 3000L;
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .settings(settings(IndexVersion.current()).put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            DataStream dataStream = new DataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex()),
                1L,
                Map.of(),
                false,
                randomBoolean(),
                false,
                randomBoolean(),
                IndexMode.STANDARD
            );
            assertThat(dataStream.getGenerationLifecycleDate(indexMetadata).millis(), is(originTimeMillis));
        }
        {
            // If the index has already rolled over and has an origination date, we always get the origination date
            IndexMetadata.Builder writeIndexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);
            long originTimeMillis = creationTimeMillis - 3000L;
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .settings(settings(IndexVersion.current()).put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);

            MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(rolloverTimeMills));
            indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            DataStream dataStream = new DataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex()),
                1L,
                Map.of(),
                false,
                randomBoolean(),
                false,
                randomBoolean(),
                IndexMode.STANDARD
            );
            assertThat(dataStream.getGenerationLifecycleDate(indexMetadata).millis(), is(originTimeMillis));
        }
        {
            // for rolled indices on other targets than the data stream name we get the origin date if origin date is set
            IndexMetadata.Builder writeIndexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);
            long originTimeMillis = creationTimeMillis - 3000L;
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .settings(settings(IndexVersion.current()).put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);

            MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(rolloverTimeMills));
            indexMetaBuilder.putRolloverInfo(new RolloverInfo("some-alias-name", List.of(rolloverCondition), now - 2000L));
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            DataStream dataStream = new DataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex()),
                1L,
                Map.of(),
                false,
                randomBoolean(),
                false,
                randomBoolean(),
                IndexMode.STANDARD
            );
            assertThat(dataStream.getGenerationLifecycleDate(indexMetadata).millis(), is(originTimeMillis));
        }
    }

    public void testGetIndicesOlderThan() {
        String dataStreamName = "metrics-foo";
        long now = System.currentTimeMillis();

        List<DataStreamMetadata> creationAndRolloverTimes = List.of(
            DataStreamMetadata.dataStreamMetadata(now - 5000, now - 4000),
            DataStreamMetadata.dataStreamMetadata(now - 4000, now - 3000),
            DataStreamMetadata.dataStreamMetadata(now - 3000, now - 2000),
            DataStreamMetadata.dataStreamMetadata(now - 2000, now - 1000),
            DataStreamMetadata.dataStreamMetadata(now, null)
        );

        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            creationAndRolloverTimes,
            settings(IndexVersion.current()),
            new DataStreamLifecycle()
        );
        Metadata metadata = builder.build();
        {
            List<Index> backingIndices = dataStream.getNonWriteIndicesOlderThan(
                TimeValue.timeValueMillis(2500),
                metadata::index,
                null,
                () -> now
            );
            assertThat(backingIndices.size(), is(2));
            assertThat(backingIndices.get(0).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
            assertThat(backingIndices.get(1).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 2)));
        }

        {
            List<Index> backingIndices = dataStream.getNonWriteIndicesOlderThan(
                TimeValue.timeValueMillis(0),
                metadata::index,
                null,
                () -> now
            );
            assertThat(backingIndices.size(), is(4));
            assertThat(backingIndices.get(0).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
            assertThat(backingIndices.get(1).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 2)));
            assertThat(backingIndices.get(2).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 3)));
            assertThat(backingIndices.get(3).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 4)));
        }

        {
            List<Index> backingIndices = dataStream.getNonWriteIndicesOlderThan(
                TimeValue.timeValueMillis(6000),
                metadata::index,
                null,
                () -> now
            );
            assertThat(backingIndices.isEmpty(), is(true));
        }

        {
            Predicate<IndexMetadata> genThreeAndFivePredicate = indexMetadata -> indexMetadata.getIndex().getName().endsWith("00003")
                || indexMetadata.getIndex().getName().endsWith("00005");

            List<Index> backingIndices = dataStream.getNonWriteIndicesOlderThan(
                TimeValue.timeValueMillis(0),
                metadata::index,
                genThreeAndFivePredicate,
                () -> now
            );
            assertThat(backingIndices.size(), is(1));
            assertThat(backingIndices.get(0).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 3)));
        }

    }

    public void testGetIndicesPastRetention() {
        String dataStreamName = "metrics-foo";
        long now = System.currentTimeMillis();

        List<DataStreamMetadata> creationAndRolloverTimes = List.of(
            DataStreamMetadata.dataStreamMetadata(now - 5000_000, now - 4000_000),
            DataStreamMetadata.dataStreamMetadata(now - 4000_000, now - 3000_000),
            DataStreamMetadata.dataStreamMetadata(now - 3000_000, now - 2000_000),
            DataStreamMetadata.dataStreamMetadata(now - 2000_000, now - 1000_000),
            DataStreamMetadata.dataStreamMetadata(now, null)
        );

        {
            // no lifecycle configured so we expect an empty list
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()),
                null
            );
            Metadata metadata = builder.build();

            assertThat(dataStream.getIndicesPastRetention(metadata::index, () -> now, randomGlobalRetention()).isEmpty(), is(true));
        }

        {
            // no retention configured but we have default retention
            DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(
                TimeValue.timeValueSeconds(2500),
                randomBoolean() ? TimeValue.timeValueSeconds(randomIntBetween(2500, 5000)) : null
            );
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()),
                new DataStreamLifecycle()
            );
            Metadata metadata = builder.build();

            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, globalRetention);
            assertThat(backingIndices.size(), is(2));
            assertThat(backingIndices.get(0).getName(), is(dataStream.getIndices().get(0).getName()));
            assertThat(backingIndices.get(1).getName(), is(dataStream.getIndices().get(1).getName()));
        }

        {
            // no retention configured but we have max retention
            DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(null, TimeValue.timeValueSeconds(2500));
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()),
                new DataStreamLifecycle()
            );
            Metadata metadata = builder.build();

            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, globalRetention);
            assertThat(backingIndices.size(), is(2));
            assertThat(backingIndices.get(0).getName(), is(dataStream.getIndices().get(0).getName()));
            assertThat(backingIndices.get(1).getName(), is(dataStream.getIndices().get(1).getName()));
        }

        {
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()),
                DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueSeconds(2500)).build()
            );
            Metadata metadata = builder.build();

            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, randomGlobalRetention());
            assertThat(backingIndices.size(), is(2));
            assertThat(backingIndices.get(0).getName(), is(dataStream.getIndices().get(0).getName()));
            assertThat(backingIndices.get(1).getName(), is(dataStream.getIndices().get(1).getName()));
        }

        {
            // even though all indices match the write index should not be returned
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()),
                DataStreamLifecycle.newBuilder().dataRetention(0).build()
            );
            Metadata metadata = builder.build();

            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, randomGlobalRetention());

            assertThat(backingIndices.size(), is(4));
            assertThat(backingIndices.get(0).getName(), is(dataStream.getIndices().get(0).getName()));
            assertThat(backingIndices.get(1).getName(), is(dataStream.getIndices().get(1).getName()));
            assertThat(backingIndices.get(2).getName(), is(dataStream.getIndices().get(2).getName()));
            assertThat(backingIndices.get(3).getName(), is(dataStream.getIndices().get(3).getName()));
        }

        {
            // no index matches the retention age
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()),
                DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueSeconds(6000)).build()
            );
            Metadata metadata = builder.build();

            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, randomGlobalRetention());
            assertThat(backingIndices.isEmpty(), is(true));
        }

        {
            // no indices are returned as even though all pass retention age none are managed by data stream lifecycle
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                Settings.builder()
                    .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
                DataStreamLifecycle.newBuilder().dataRetention(0).build()
            );
            Metadata metadata = builder.build();

            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, randomGlobalRetention());
            assertThat(backingIndices.isEmpty(), is(true));
        }
    }

    public void testGetIndicesPastRetentionWithOriginationDate() {
        // First, build an ordinary data stream:
        String dataStreamName = "metrics-foo";
        long now = System.currentTimeMillis();
        List<DataStreamMetadata> creationAndRolloverTimes = List.of(
            DataStreamMetadata.dataStreamMetadata(now - 5000, now - 4000),
            DataStreamMetadata.dataStreamMetadata(now - 4000, now - 3000),
            DataStreamMetadata.dataStreamMetadata(now - 3000, now - 2000),
            DataStreamMetadata.dataStreamMetadata(now - 2000, now - 1000),
            DataStreamMetadata.dataStreamMetadata(now, null, now - 8000), // origination date older than retention
            DataStreamMetadata.dataStreamMetadata(now, null, now - 1000), // origination date within retention
            DataStreamMetadata.dataStreamMetadata(now, null)
        );
        Metadata.Builder metadataBuilder = Metadata.builder();
        AtomicReference<TimeValue> testRetentionReference = new AtomicReference<>(null);
        DataStream dataStream = createDataStream(
            metadataBuilder,
            dataStreamName,
            creationAndRolloverTimes,
            settings(IndexVersion.current()),
            new DataStreamLifecycle() {
                public TimeValue getDataStreamRetention() {
                    return testRetentionReference.get();
                }
            }
        );
        Metadata metadata = metadataBuilder.build();
        {
            // no retention configured so we expect an empty list
            testRetentionReference.set(null);
            assertThat(dataStream.getIndicesPastRetention(metadata::index, () -> now, null).isEmpty(), is(true));
        }

        {
            // retention period where oldIndex is too old, but newIndex should be retained
            testRetentionReference.set(TimeValue.timeValueMillis(2500));
            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, null);
            assertThat(backingIndices.size(), is(3));
            assertThat(backingIndices.get(0).getName(), is(dataStream.getIndices().get(0).getName()));
            assertThat(backingIndices.get(1).getName(), is(dataStream.getIndices().get(1).getName()));
            assertThat(backingIndices.get(2).getName(), is(dataStream.getIndices().get(5).getName()));
        }

        {
            // even though all indices match the write index should not be returned
            testRetentionReference.set(TimeValue.timeValueMillis(0));
            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, null);

            assertThat(backingIndices.size(), is(6));
            assertThat(backingIndices.get(0).getName(), is(dataStream.getIndices().get(0).getName()));
            assertThat(backingIndices.get(1).getName(), is(dataStream.getIndices().get(1).getName()));
            assertThat(backingIndices.get(2).getName(), is(dataStream.getIndices().get(2).getName()));
            assertThat(backingIndices.get(3).getName(), is(dataStream.getIndices().get(3).getName()));
            assertThat(backingIndices.get(4).getName(), is(dataStream.getIndices().get(4).getName()));
            assertThat(backingIndices.get(5).getName(), is(dataStream.getIndices().get(5).getName()));
        }

        {
            // no index matches the retention age
            testRetentionReference.set(TimeValue.timeValueMillis(9000));
            List<Index> backingIndices = dataStream.getIndicesPastRetention(metadata::index, () -> now, null);
            assertThat(backingIndices.isEmpty(), is(true));
        }
    }

    public void testGetDownsampleRounds() {
        String dataStreamName = "metrics-foo";
        long now = System.currentTimeMillis();

        List<DataStreamMetadata> creationAndRolloverTimes = List.of(
            DataStreamMetadata.dataStreamMetadata(now - 5000, now - 4000),
            DataStreamMetadata.dataStreamMetadata(now - 4000, now - 3000),
            DataStreamMetadata.dataStreamMetadata(now - 3000, now - 2000),
            DataStreamMetadata.dataStreamMetadata(now - 2000, now - 1000),
            DataStreamMetadata.dataStreamMetadata(now, null)
        );

        {
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .put("index.routing_path", "@timestamp"),
                DataStreamLifecycle.newBuilder()
                    .downsampling(
                        new DataStreamLifecycle.Downsampling(
                            List.of(
                                new DataStreamLifecycle.Downsampling.Round(
                                    TimeValue.timeValueMillis(2000),
                                    new DownsampleConfig(new DateHistogramInterval("10m"))
                                ),
                                new DataStreamLifecycle.Downsampling.Round(
                                    TimeValue.timeValueMillis(3200),
                                    new DownsampleConfig(new DateHistogramInterval("100m"))
                                ),
                                new DataStreamLifecycle.Downsampling.Round(
                                    TimeValue.timeValueMillis(3500),
                                    new DownsampleConfig(new DateHistogramInterval("1000m"))
                                )
                            )
                        )
                    )
                    .build()
            );
            Metadata metadata = builder.build();

            // generation time is now - 2000
            String thirdGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 3);
            Index thirdIndex = metadata.index(thirdGeneration).getIndex();
            List<DataStreamLifecycle.Downsampling.Round> roundsForThirdIndex = dataStream.getDownsamplingRoundsFor(
                thirdIndex,
                metadata::index,
                () -> now
            );

            assertThat(roundsForThirdIndex.size(), is(1));
            assertThat(roundsForThirdIndex.get(0).after(), is(TimeValue.timeValueMillis(2000)));

            // generation time is now - 40000
            String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            Index firstIndex = metadata.index(firstGeneration).getIndex();
            List<DataStreamLifecycle.Downsampling.Round> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata::index,
                () -> now
            );
            // expecting all rounds to match this index
            assertThat(roundsForFirstIndex.size(), is(3));
            // assert the order is maintained
            assertThat(
                roundsForFirstIndex.stream().map(DataStreamLifecycle.Downsampling.Round::after).toList(),
                is(List.of(TimeValue.timeValueMillis(2000), TimeValue.timeValueMillis(3200), TimeValue.timeValueMillis(3500)))
            );
        }

        {
            // non-timeseries indices should be skipped
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                // no TSDB settings
                settings(IndexVersion.current()),
                DataStreamLifecycle.newBuilder()
                    .downsampling(
                        new DataStreamLifecycle.Downsampling(
                            List.of(
                                new DataStreamLifecycle.Downsampling.Round(
                                    TimeValue.timeValueMillis(2000),
                                    new DownsampleConfig(new DateHistogramInterval("10m"))
                                ),
                                new DataStreamLifecycle.Downsampling.Round(
                                    TimeValue.timeValueMillis(3200),
                                    new DownsampleConfig(new DateHistogramInterval("100m"))
                                ),
                                new DataStreamLifecycle.Downsampling.Round(
                                    TimeValue.timeValueMillis(3500),
                                    new DownsampleConfig(new DateHistogramInterval("1000m"))
                                )
                            )
                        )
                    )
                    .build()
            );
            Metadata metadata = builder.build();

            // generation time is now - 40000
            String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            Index firstIndex = metadata.index(firstGeneration).getIndex();
            List<DataStreamLifecycle.Downsampling.Round> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata::index,
                () -> now
            );
            assertThat(roundsForFirstIndex.size(), is(0));
        }

        {
            // backing indices for data streams without lifecycle don't match any rounds
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .put("index.routing_path", "@timestamp"),
                null
            );
            Metadata metadata = builder.build();

            // generation time is now - 40000
            String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            Index firstIndex = metadata.index(firstGeneration).getIndex();
            List<DataStreamLifecycle.Downsampling.Round> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata::index,
                () -> now
            );
            assertThat(roundsForFirstIndex.size(), is(0));
        }

        {
            // backing indices for data streams without downsampling configured don't match any rounds
            Metadata.Builder builder = Metadata.builder();
            DataStream dataStream = createDataStream(
                builder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .put("index.routing_path", "@timestamp"),
                DataStreamLifecycle.newBuilder().build()
            );
            Metadata metadata = builder.build();

            String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            Index firstIndex = metadata.index(firstGeneration).getIndex();
            List<DataStreamLifecycle.Downsampling.Round> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata::index,
                () -> now
            );
            assertThat(roundsForFirstIndex.size(), is(0));
        }
    }

    public void testIsIndexManagedByDataStreamLifecycle() {
        String dataStreamName = "metrics-foo";
        long now = System.currentTimeMillis();

        List<DataStreamMetadata> creationAndRolloverTimes = List.of(
            DataStreamMetadata.dataStreamMetadata(now - 5000, now - 4000),
            DataStreamMetadata.dataStreamMetadata(now - 4000, now - 3000),
            DataStreamMetadata.dataStreamMetadata(now - 3000, now - 2000),
            DataStreamMetadata.dataStreamMetadata(now - 2000, now - 1000),
            DataStreamMetadata.dataStreamMetadata(now, null)
        );
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            creationAndRolloverTimes,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(0).build()
        );
        Metadata metadata = builder.build();

        {
            // false for indices not part of the data stream
            assertThat(dataStream.isIndexManagedByDataStreamLifecycle(new Index("standalone_index", "uuid"), metadata::index), is(false));
        }

        {
            // false for indices that were deleted
            assertThat(dataStream.isIndexManagedByDataStreamLifecycle(dataStream.getIndices().get(1), (index) -> null), is(false));
        }

        {
            // false if data stream doesn't have a lifecycle
            Metadata.Builder newBuilder = Metadata.builder();
            DataStream unmanagedDataStream = createDataStream(
                newBuilder,
                dataStreamName,
                creationAndRolloverTimes,
                settings(IndexVersion.current()),
                null
            );
            Metadata newMetadata = newBuilder.build();
            assertThat(
                unmanagedDataStream.isIndexManagedByDataStreamLifecycle(unmanagedDataStream.getIndices().get(1), newMetadata::index),
                is(false)
            );
        }

        {
            // false for indices that have an ILM policy configured
            Metadata.Builder builderWithIlm = Metadata.builder();
            DataStream ds = createDataStream(
                builderWithIlm,
                dataStreamName,
                creationAndRolloverTimes,
                Settings.builder()
                    .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
                new DataStreamLifecycle()
            );
            Metadata metadataIlm = builderWithIlm.build();
            for (Index index : ds.getIndices()) {
                assertThat(ds.isIndexManagedByDataStreamLifecycle(index, metadataIlm::index), is(false));
            }
        }

        {
            // true for indices that have an ILM policy configured AND the prefer_ilm setting configured to false
            {
                // false for indices that have an ILM policy configured
                Metadata.Builder builderWithIlm = Metadata.builder();
                DataStream ds = createDataStream(
                    builderWithIlm,
                    dataStreamName,
                    creationAndRolloverTimes,
                    Settings.builder()
                        .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexSettings.PREFER_ILM, false),
                    new DataStreamLifecycle()
                );
                Metadata metadataIlm = builderWithIlm.build();
                for (Index index : ds.getIndices()) {
                    assertThat(ds.isIndexManagedByDataStreamLifecycle(index, metadataIlm::index), is(true));
                }
            }
        }

        {
            // true otherwise
            for (Index index : dataStream.getIndices()) {
                assertThat(dataStream.isIndexManagedByDataStreamLifecycle(index, metadata::index), is(true));
            }
        }
    }

    public void testGetIndicesOlderThanWithOriginationDate() {
        // First, build an ordinary datastream:
        String dataStreamName = "metrics-foo";
        long now = System.currentTimeMillis();
        List<DataStreamMetadata> creationAndRolloverTimes = List.of(
            DataStreamMetadata.dataStreamMetadata(now - 5000, now - 4000),
            DataStreamMetadata.dataStreamMetadata(now - 4000, now - 3000),
            DataStreamMetadata.dataStreamMetadata(now - 3000, now - 2000),
            DataStreamMetadata.dataStreamMetadata(now - 2000, now - 1000),
            DataStreamMetadata.dataStreamMetadata(now, null, now - 7000), // origination date older than retention
            DataStreamMetadata.dataStreamMetadata(now, null, now - 1000), // origination date within retention
            DataStreamMetadata.dataStreamMetadata(now, null, now - 7000) // write index origination date older than retention
        );
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            creationAndRolloverTimes,
            settings(IndexVersion.current()),
            new DataStreamLifecycle()
        );
        Metadata metadata = builder.build();

        List<Index> backingIndices = dataStream.getNonWriteIndicesOlderThan(
            TimeValue.timeValueMillis(2500),
            metadata::index,
            null,
            () -> now
        );
        // We expect to see the index with the really old origination date, but not the one with the more recent origination date (and
        // not the write index)
        assertThat(backingIndices.size(), is(3));
        assertThat(backingIndices.get(0).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
        assertThat(backingIndices.get(1).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 2)));
        assertThat(backingIndices.get(2).getName(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 6)));
    }

    private DataStream createDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        List<DataStreamMetadata> creationAndRolloverTimes,
        Settings.Builder backingIndicesSettings,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        int backingIndicesCount = creationAndRolloverTimes.size();
        final List<Index> backingIndices = new ArrayList<>();
        for (int k = 1; k <= backingIndicesCount; k++) {
            DataStreamMetadata creationRolloverTime = creationAndRolloverTimes.get(k - 1);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k))
                .settings(backingIndicesSettings)
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationRolloverTime.creationTimeInMillis());
            if (k < backingIndicesCount) {
                // add rollover info only for non-write indices
                Long rolloverTimeMillis = creationRolloverTime.rolloverTimeInMillis();
                if (rolloverTimeMillis != null) {
                    MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(rolloverTimeMillis));
                    indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), rolloverTimeMillis));
                }
            }
            Long originationTimeInMillis = creationRolloverTime.originationTimeInMillis;
            if (originationTimeInMillis != null) {
                backingIndicesSettings.put(LIFECYCLE_ORIGINATION_DATE, originationTimeInMillis);
            }
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            backingIndices.add(indexMetadata.getIndex());
        }
        return newInstance(dataStreamName, backingIndices, backingIndicesCount, null, false, lifecycle);
    }

    public void testXContentSerializationWithRolloverAndEffectiveRetention() throws IOException {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        List<Index> indices = randomIndexInstances();
        long generation = indices.size() + ESTestCase.randomLongBetween(1, 128);
        indices.add(new Index(getDefaultBackingIndexName(dataStreamName, generation), UUIDs.randomBase64UUID(LuceneTestCase.random())));
        Map<String, Object> metadata = null;
        if (randomBoolean()) {
            metadata = Map.of("key", "value");
        }
        boolean failureStore = randomBoolean();
        List<Index> failureIndices = List.of();
        if (failureStore) {
            failureIndices = randomNonEmptyIndexInstances();
        }

        DataStreamLifecycle lifecycle = new DataStreamLifecycle();
        DataStream dataStream = new DataStream(
            dataStreamName,
            indices,
            generation,
            metadata,
            randomBoolean(),
            randomBoolean(),
            false, // Some tests don't work well with system data streams, since these data streams require special handling
            System::currentTimeMillis,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : null, // IndexMode.TIME_SERIES triggers validation that many unit tests doesn't pass
            lifecycle,
            failureStore,
            failureIndices,
            false,
            null
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            RolloverConfiguration rolloverConfiguration = RolloverConfigurationTests.randomRolloverConditions();
            DataStreamGlobalRetention globalRetention = DataStreamGlobalRetentionTests.randomGlobalRetention();

            ToXContent.Params withEffectiveRetention = new ToXContent.MapParams(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS);
            dataStream.toXContent(builder, withEffectiveRetention, rolloverConfiguration, globalRetention);
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("rollover"));
            for (String label : rolloverConfiguration.resolveRolloverConditions(lifecycle.getEffectiveDataRetention(globalRetention))
                .getConditions()
                .keySet()) {
                assertThat(serialized, containsString(label));
            }
            // We check that even if there was no retention provided by the user, the global retention applies
            assertThat(serialized, not(containsString("data_retention")));
            assertThat(serialized, containsString("effective_retention"));
        }
    }

    public void testGetIndicesWithinMaxAgeRange() {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final int numberOfBackingIndicesOlderThanMinAge = randomIntBetween(0, 10);
        final int numberOfBackingIndicesWithinMinAnge = randomIntBetween(0, 10);
        final int numberOfShards = 1;
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "logs-es";
        final List<Index> backingIndicesOlderThanMinAge = new ArrayList<>();
        for (int i = 0; i < numberOfBackingIndicesOlderThanMinAge; i++) {
            long creationDate = System.currentTimeMillis() - maxIndexAge.millis() * 2;
            final IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), creationDate),
                randomIndexWriteLoad(numberOfShards),
                creationDate
            );
            backingIndices.add(indexMetadata.getIndex());
            backingIndicesOlderThanMinAge.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final List<Index> backingIndicesWithinMinAge = new ArrayList<>();
        for (int i = 0; i < numberOfBackingIndicesWithinMinAnge; i++) {
            final long createdAt = System.currentTimeMillis() - (maxIndexAge.getMillis() / 2);
            final IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), createdAt),
                randomIndexWriteLoad(numberOfShards),
                createdAt
            );
            backingIndices.add(indexMetadata.getIndex());
            backingIndicesWithinMinAge.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size());
        final IndexMetadata writeIndexMetadata = createIndexMetadata(writeIndexName, null, System.currentTimeMillis());
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = new DataStream(
            dataStreamName,
            backingIndices,
            backingIndices.size(),
            Collections.emptyMap(),
            false,
            false,
            false,
            false,
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES
        );

        metadataBuilder.put(dataStream);

        final List<Index> indicesWithinMaxAgeRange = DataStream.getIndicesWithinMaxAgeRange(
            dataStream,
            metadataBuilder::getSafe,
            maxIndexAge,
            System::currentTimeMillis
        );

        final List<Index> expectedIndicesWithinMaxAgeRange = new ArrayList<>();
        if (numberOfBackingIndicesOlderThanMinAge > 0) {
            expectedIndicesWithinMaxAgeRange.add(backingIndicesOlderThanMinAge.get(backingIndicesOlderThanMinAge.size() - 1));
        }
        expectedIndicesWithinMaxAgeRange.addAll(backingIndicesWithinMinAge);
        expectedIndicesWithinMaxAgeRange.add(writeIndexMetadata.getIndex());

        assertThat(indicesWithinMaxAgeRange, is(equalTo(expectedIndicesWithinMaxAgeRange)));
    }

    public void testGetIndicesWithinMaxAgeRangeAllIndicesOutsideRange() {
        final TimeValue maxIndexAge = TimeValue.timeValueDays(7);

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final int numberOfBackingIndicesOlderThanMinAge = randomIntBetween(5, 10);
        final int numberOfShards = 1;
        final List<Index> backingIndices = new ArrayList<>();
        final String dataStreamName = "logs-es";
        final List<Index> backingIndicesOlderThanMinAge = new ArrayList<>();
        for (int i = 0; i < numberOfBackingIndicesOlderThanMinAge; i++) {
            long creationDate = System.currentTimeMillis() - maxIndexAge.millis() * 2;
            final IndexMetadata indexMetadata = createIndexMetadata(
                DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size(), creationDate),
                randomIndexWriteLoad(numberOfShards),
                creationDate
            );
            backingIndices.add(indexMetadata.getIndex());
            backingIndicesOlderThanMinAge.add(indexMetadata.getIndex());
            metadataBuilder.put(indexMetadata, false);
        }

        final String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndices.size());
        final IndexMetadata writeIndexMetadata = createIndexMetadata(
            writeIndexName,
            null,
            System.currentTimeMillis() - maxIndexAge.millis() * 2
        );
        backingIndices.add(writeIndexMetadata.getIndex());
        metadataBuilder.put(writeIndexMetadata, false);

        final DataStream dataStream = new DataStream(
            dataStreamName,
            backingIndices,
            backingIndices.size(),
            Collections.emptyMap(),
            false,
            false,
            false,
            false,
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES
        );

        metadataBuilder.put(dataStream);

        final List<Index> indicesWithinMaxAgeRange = DataStream.getIndicesWithinMaxAgeRange(
            dataStream,
            metadataBuilder::getSafe,
            maxIndexAge,
            System::currentTimeMillis
        );

        final List<Index> expectedIndicesWithinMaxAgeRange = new ArrayList<>();
        if (numberOfBackingIndicesOlderThanMinAge > 0) {
            expectedIndicesWithinMaxAgeRange.add(backingIndicesOlderThanMinAge.get(backingIndicesOlderThanMinAge.size() - 1));
        }
        expectedIndicesWithinMaxAgeRange.add(writeIndexMetadata.getIndex());
        assertThat(indicesWithinMaxAgeRange, is(equalTo(expectedIndicesWithinMaxAgeRange)));
        assertThat(indicesWithinMaxAgeRange.get(indicesWithinMaxAgeRange.size() - 1).getName(), is(writeIndexName));
    }

    private IndexWriteLoad randomIndexWriteLoad(int numberOfShards) {
        IndexWriteLoad.Builder builder = IndexWriteLoad.builder(numberOfShards);
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            builder.withShardWriteLoad(shardId, randomDoubleBetween(0, 64, true), randomLongBetween(1, 10));
        }
        return builder.build();
    }

    private IndexMetadata createIndexMetadata(String indexName, IndexWriteLoad indexWriteLoad, long createdAt) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .build()
            )
            .stats(indexWriteLoad == null ? null : new IndexMetadataStats(indexWriteLoad, 1, 1))
            .creationDate(createdAt)
            .build();
    }

    public void testWriteFailureIndex() {
        boolean hidden = randomBoolean();
        boolean system = hidden && randomBoolean();
        DataStream noFailureStoreDataStream = new DataStream(
            randomAlphaOfLength(10),
            randomNonEmptyIndexInstances(),
            randomNonNegativeInt(),
            null,
            hidden,
            randomBoolean(),
            system,
            System::currentTimeMillis,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomLifecycle(),
            false,
            null,
            randomBoolean(),
            null
        );
        assertThat(noFailureStoreDataStream.getFailureStoreWriteIndex(), nullValue());

        DataStream failureStoreDataStreamWithEmptyFailureIndices = new DataStream(
            randomAlphaOfLength(10),
            randomNonEmptyIndexInstances(),
            randomNonNegativeInt(),
            null,
            hidden,
            randomBoolean(),
            system,
            System::currentTimeMillis,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomLifecycle(),
            true,
            List.of(),
            randomBoolean(),
            null
        );
        assertThat(failureStoreDataStreamWithEmptyFailureIndices.getFailureStoreWriteIndex(), nullValue());

        List<Index> failureIndices = randomIndexInstances();
        String dataStreamName = randomAlphaOfLength(10);
        Index writeFailureIndex = new Index(
            getDefaultBackingIndexName(dataStreamName, randomNonNegativeInt()),
            UUIDs.randomBase64UUID(LuceneTestCase.random())
        );
        failureIndices.add(writeFailureIndex);
        DataStream failureStoreDataStream = new DataStream(
            dataStreamName,
            randomNonEmptyIndexInstances(),
            randomNonNegativeInt(),
            null,
            hidden,
            randomBoolean(),
            system,
            System::currentTimeMillis,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomLifecycle(),
            true,
            failureIndices,
            randomBoolean(),
            null
        );
        assertThat(failureStoreDataStream.getFailureStoreWriteIndex(), is(writeFailureIndex));
    }

    public void testIsFailureIndex() {
        boolean hidden = randomBoolean();
        boolean system = hidden && randomBoolean();
        List<Index> backingIndices = randomNonEmptyIndexInstances();
        DataStream noFailureStoreDataStream = new DataStream(
            randomAlphaOfLength(10),
            backingIndices,
            randomNonNegativeInt(),
            null,
            hidden,
            randomBoolean(),
            system,
            System::currentTimeMillis,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomLifecycle(),
            false,
            null,
            randomBoolean(),
            null
        );
        assertThat(
            noFailureStoreDataStream.isFailureStoreIndex(backingIndices.get(randomIntBetween(0, backingIndices.size() - 1)).getName()),
            is(false)
        );

        backingIndices = randomNonEmptyIndexInstances();
        DataStream failureStoreDataStreamWithEmptyFailureIndices = new DataStream(
            randomAlphaOfLength(10),
            backingIndices,
            randomNonNegativeInt(),
            null,
            hidden,
            randomBoolean(),
            system,
            System::currentTimeMillis,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomLifecycle(),
            true,
            List.of(),
            randomBoolean(),
            null
        );
        assertThat(
            failureStoreDataStreamWithEmptyFailureIndices.isFailureStoreIndex(
                backingIndices.get(randomIntBetween(0, backingIndices.size() - 1)).getName()
            ),
            is(false)
        );

        backingIndices = randomNonEmptyIndexInstances();
        List<Index> failureIndices = randomIndexInstances();
        String dataStreamName = randomAlphaOfLength(10);
        Index writeFailureIndex = new Index(
            getDefaultBackingIndexName(dataStreamName, randomNonNegativeInt()),
            UUIDs.randomBase64UUID(LuceneTestCase.random())
        );
        failureIndices.add(writeFailureIndex);
        DataStream failureStoreDataStream = new DataStream(
            dataStreamName,
            backingIndices,
            randomNonNegativeInt(),
            null,
            hidden,
            randomBoolean(),
            system,
            System::currentTimeMillis,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomLifecycle(),
            true,
            failureIndices,
            randomBoolean(),
            null
        );
        assertThat(failureStoreDataStream.isFailureStoreIndex(writeFailureIndex.getName()), is(true));
        assertThat(
            failureStoreDataStream.isFailureStoreIndex(failureIndices.get(randomIntBetween(0, failureIndices.size() - 1)).getName()),
            is(true)
        );
        assertThat(
            failureStoreDataStreamWithEmptyFailureIndices.isFailureStoreIndex(
                backingIndices.get(randomIntBetween(0, backingIndices.size() - 1)).getName()
            ),
            is(false)
        );
        assertThat(failureStoreDataStreamWithEmptyFailureIndices.isFailureStoreIndex(randomAlphaOfLength(10)), is(false));
    }

    private record DataStreamMetadata(Long creationTimeInMillis, Long rolloverTimeInMillis, Long originationTimeInMillis) {
        public static DataStreamMetadata dataStreamMetadata(Long creationTimeInMillis, Long rolloverTimeInMillis) {
            return new DataStreamMetadata(creationTimeInMillis, rolloverTimeInMillis, null);
        }

        public static DataStreamMetadata dataStreamMetadata(
            Long creationTimeInMillis,
            Long rolloverTimeInMillis,
            Long originationTimeInMillis
        ) {
            return new DataStreamMetadata(creationTimeInMillis, rolloverTimeInMillis, originationTimeInMillis);
        }
    }
}
