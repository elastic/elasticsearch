/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.indices.SystemIndices;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomMappings;
import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.DataStream.getDefaultFailureStoreName;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.randomIndexInstances;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.randomNonEmptyIndexInstances;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.randomSettings;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataStreamTests extends AbstractXContentSerializingTestCase<DataStream> {

    @Override
    protected DataStream doParseInstance(XContentParser parser) throws IOException {
        return DataStream.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStream> instanceReader() {
        return DataStream::read;
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
        var settings = instance.getSettings();
        var isHidden = instance.isHidden();
        var isReplicated = instance.isReplicated();
        var isSystem = instance.isSystem();
        var allowsCustomRouting = instance.isAllowCustomRouting();
        var indexMode = instance.getIndexMode();
        var lifecycle = instance.getDataLifecycle();
        var dataStreamOptions = instance.getDataStreamOptions();
        var failureIndices = instance.getFailureIndices();
        var rolloverOnWrite = instance.rolloverOnWrite();
        var autoShardingEvent = instance.getAutoShardingEvent();
        var failureRolloverOnWrite = instance.getFailureComponent().isRolloverOnWrite();
        var failureAutoShardingEvent = instance.getDataComponent().getAutoShardingEvent();
        switch (between(0, 16)) {
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
            case 5 -> {
                isReplicated = isReplicated == false;
                // Replicated data streams cannot be marked for lazy rollover.
                rolloverOnWrite = isReplicated == false && rolloverOnWrite;
                failureRolloverOnWrite = isReplicated == false && failureRolloverOnWrite;
            }
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
                : DataStreamLifecycle.dataLifecycleBuilder().dataRetention(randomPositiveTimeValue()).build();
            case 10 -> failureIndices = randomValueOtherThan(failureIndices, DataStreamTestHelper::randomIndexInstances);
            case 11 -> dataStreamOptions = dataStreamOptions.isEmpty()
                ? new DataStreamOptions(DataStreamFailureStoreTests.randomFailureStore())
                : randomBoolean() ? DataStreamOptions.EMPTY
                : new DataStreamOptions(
                    randomValueOtherThan(dataStreamOptions.failureStore(), DataStreamFailureStoreTests::randomFailureStore)
                );
            case 12 -> {
                rolloverOnWrite = rolloverOnWrite == false;
                isReplicated = rolloverOnWrite == false && isReplicated;
            }
            case 13 -> {
                if (randomBoolean() || autoShardingEvent == null) {
                    // If we're mutating the auto sharding event of the failure store, we need to ensure there's at least one failure index.
                    if (failureIndices.isEmpty()) {
                        failureIndices = DataStreamTestHelper.randomNonEmptyIndexInstances();
                        dataStreamOptions = DataStreamOptions.FAILURE_STORE_ENABLED;
                    }
                    autoShardingEvent = new DataStreamAutoShardingEvent(
                        failureIndices.getLast().getName(),
                        randomIntBetween(1, 10),
                        randomMillisUpToYear9999()
                    );
                } else {
                    autoShardingEvent = null;
                }
            }
            case 14 -> {
                failureRolloverOnWrite = failureRolloverOnWrite == false;
                isReplicated = failureRolloverOnWrite == false && isReplicated;
            }
            case 15 -> failureAutoShardingEvent = randomBoolean() && failureAutoShardingEvent != null
                ? null
                : new DataStreamAutoShardingEvent(indices.getLast().getName(), randomIntBetween(1, 10), randomMillisUpToYear9999());
            case 16 -> settings = randomValueOtherThan(settings, DataStreamTestHelper::randomSettings);
        }

        return new DataStream(
            name,
            generation,
            metadata,
            settings,
            isHidden,
            isReplicated,
            isSystem,
            System::currentTimeMillis,
            allowsCustomRouting,
            indexMode,
            lifecycle,
            dataStreamOptions,
            new DataStream.DataStreamIndices(DataStream.BACKING_INDEX_PREFIX, indices, rolloverOnWrite, autoShardingEvent),
            new DataStream.DataStreamIndices(
                DataStream.BACKING_INDEX_PREFIX,
                failureIndices,
                failureRolloverOnWrite,
                failureAutoShardingEvent
            )
        );
    }

    public void testRollover() {
        DataStream ds = DataStreamTestHelper.randomInstance().promoteDataStream();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        Tuple<String, Long> newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getDataComponent());
        final DataStream rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), null, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        // Irrespective of whether the rollover was performed lazily, rolloverOnWrite should always be set to false after rollover.
        assertFalse(rolledDs.rolloverOnWrite());
    }

    public void testRolloverWithConflictingBackingIndexName() {
        // used a fixed time provider to guarantee name conflicts
        DataStream ds = DataStreamTestHelper.randomInstance(() -> 0L).promoteDataStream();

        // create some indices with names that conflict with the names of the data stream's backing indices
        int numConflictingIndices = randomIntBetween(1, 10);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        for (int k = 1; k <= numConflictingIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(ds.getName(), ds.getGeneration() + k, 0L))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }

        final Tuple<String, Long> newCoordinates = ds.nextWriteIndexAndGeneration(builder.build(), ds.getDataComponent());
        final DataStream rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), null, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + numConflictingIndices + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), equalTo(ds.getIndexMode()));
    }

    public void testRolloverUpgradeToTsdbDataStream() {
        DataStream ds = DataStreamTestHelper.randomInstance()
            .copy()
            .setReplicated(false)
            .setIndexMode(randomBoolean() ? IndexMode.STANDARD : null)
            .build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        var newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getDataComponent());

        var rolledDs = ds.rollover(
            new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()),
            newCoordinates.v2(),
            IndexMode.TIME_SERIES,
            null
        );
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), equalTo(IndexMode.TIME_SERIES));
    }

    public void testRolloverUpgradeToLogsdbDataStream() {
        DataStream ds = DataStreamTestHelper.randomInstance()
            .copy()
            .setReplicated(false)
            .setIndexMode(randomBoolean() ? IndexMode.STANDARD : null)
            .build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        var newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getDataComponent());

        var rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), IndexMode.LOGSDB, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), equalTo(IndexMode.LOGSDB));
    }

    public void testRolloverFromTSdbToLogsdb() {
        DataStream ds = DataStreamTestHelper.randomInstance().copy().setReplicated(false).setIndexMode(IndexMode.TIME_SERIES).build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        var newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getDataComponent());

        var rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), IndexMode.LOGSDB, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), equalTo(IndexMode.LOGSDB));
    }

    public void testRolloverFromLogsdbToTsdb() {
        DataStream ds = DataStreamTestHelper.randomInstance().copy().setReplicated(false).setIndexMode(IndexMode.LOGSDB).build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        var newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getDataComponent());

        var rolledDs = ds.rollover(
            new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()),
            newCoordinates.v2(),
            IndexMode.TIME_SERIES,
            null
        );
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), equalTo(IndexMode.TIME_SERIES));
    }

    public void testRolloverDowngradeFromTsdbToRegularDataStream() {
        DataStream ds = DataStreamTestHelper.randomInstance().copy().setReplicated(false).setIndexMode(IndexMode.TIME_SERIES).build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        var newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getDataComponent());

        var rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), null, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), nullValue());
    }

    public void testRolloverDowngradeFromLogsdbToRegularDataStream() {
        DataStream ds = DataStreamTestHelper.randomInstance().copy().setReplicated(false).setIndexMode(IndexMode.LOGSDB).build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        var newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getDataComponent());

        var rolledDs = ds.rollover(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2(), null, null);
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertThat(rolledDs.getIndexMode(), nullValue());
    }

    public void testRolloverFailureStore() {
        DataStream ds = DataStreamTestHelper.randomInstance(true).promoteDataStream();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        Tuple<String, Long> newCoordinates = ds.nextWriteIndexAndGeneration(project, ds.getFailureComponent());
        final DataStream rolledDs = ds.rolloverFailureStore(new Index(newCoordinates.v1(), UUIDs.randomBase64UUID()), newCoordinates.v2());
        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size()));
        // Ensure that the rolloverOnWrite flag hasn't changed when rolling over a failure store.
        assertThat(rolledDs.rolloverOnWrite(), equalTo(ds.rolloverOnWrite()));
        assertThat(rolledDs.getFailureIndices().size(), equalTo(ds.getFailureIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(rolledDs.getWriteIndex()));
        assertTrue(rolledDs.getFailureIndices().containsAll(ds.getFailureIndices()));
        assertTrue(rolledDs.getFailureIndices().contains(rolledDs.getWriteFailureIndex()));
    }

    public void testRemoveBackingIndex() {
        DataStream original = createRandomDataStream();
        int indexToRemove = randomIntBetween(1, original.getIndices().size() - 1);

        DataStream updated = original.removeBackingIndex(original.getIndices().get(indexToRemove - 1));
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size() - 1));
        for (int k = 0; k < (original.getIndices().size() - 1); k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k < (indexToRemove - 1) ? k : k + 1)));
        }
    }

    public void testRemoveBackingIndexThatDoesNotExist() {
        DataStream original = createRandomDataStream();
        final Index indexToRemove = new Index(randomAlphaOfLength(4), UUIDs.randomBase64UUID(random()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> original.removeBackingIndex(indexToRemove));
        assertThat(
            e.getMessage(),
            equalTo(Strings.format("index [%s] is not part of data stream [%s]", indexToRemove.getName(), original.getName()))
        );
    }

    public void testRemoveBackingWriteIndex() {
        DataStream original = createRandomDataStream();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> original.removeBackingIndex(original.getIndices().get(original.getIndices().size() - 1))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot remove backing index [%s] of data stream [%s] because it is the write index",
                    original.getIndices().get(original.getIndices().size() - 1).getName(),
                    original.getName()
                )
            )
        );
    }

    public void testRemoveFailureStoreIndex() {
        DataStream original = createRandomDataStream();
        int indexToRemove = randomIntBetween(1, original.getFailureIndices().size() - 1);

        DataStream updated = original.removeFailureStoreIndex(original.getFailureIndices().get(indexToRemove - 1));
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size()));
        assertThat(updated.getFailureIndices().size(), equalTo(original.getFailureIndices().size() - 1));
        for (int k = 0; k < (original.getFailureIndices().size() - 1); k++) {
            assertThat(updated.getFailureIndices().get(k), equalTo(original.getFailureIndices().get(k < (indexToRemove - 1) ? k : k + 1)));
        }
    }

    public void testRemoveFailureStoreIndexThatDoesNotExist() {
        DataStream original = createRandomDataStream();
        final Index indexToRemove = new Index(randomAlphaOfLength(4), UUIDs.randomBase64UUID(random()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> original.removeFailureStoreIndex(indexToRemove));
        assertThat(
            e.getMessage(),
            equalTo(Strings.format("index [%s] is not part of data stream [%s] failure store", indexToRemove.getName(), original.getName()))
        );
    }

    public void testRemoveFailureStoreWriteIndex() {
        DataStream original = createRandomDataStream();
        int indexToRemove = original.getFailureIndices().size() - 1;

        DataStream updated = original.removeFailureStoreIndex(original.getFailureIndices().get(indexToRemove));
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size()));
        assertThat(updated.getFailureIndices().size(), equalTo(original.getFailureIndices().size() - 1));
        assertThat(updated.getFailureComponent().isRolloverOnWrite(), equalTo(true));
        for (int k = 0; k < (original.getFailureIndices().size() - 1); k++) {
            assertThat(updated.getFailureIndices().get(k), equalTo(original.getFailureIndices().get(k)));
        }
    }

    public void testAddBackingIndex() {
        Metadata.Builder builder = Metadata.builder();

        DataStream original = createRandomDataStream();
        builder.put(original);

        createMetadataForIndices(builder, original.getIndices());

        Index indexToAdd = new Index(randomAlphaOfLength(4), UUIDs.randomBase64UUID(random()));
        builder.put(
            IndexMetadata.builder(indexToAdd.getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(),
            false
        );

        DataStream updated = original.addBackingIndex(builder.build().getProject(), indexToAdd);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size() + 1));
        for (int k = 1; k <= original.getIndices().size(); k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k - 1)));
        }
        assertThat(updated.getIndices().get(0), equalTo(indexToAdd));
    }

    public void testAddBackingIndexThatIsPartOfAnotherDataStream() {
        Metadata.Builder builder = Metadata.builder();

        DataStream ds1 = createRandomDataStream();
        DataStream ds2 = createRandomDataStream();

        builder.put(ds1);
        builder.put(ds2);

        createMetadataForIndices(builder, ds1.getIndices());
        createMetadataForIndices(builder, ds1.getFailureIndices());
        createMetadataForIndices(builder, ds2.getIndices());
        createMetadataForIndices(builder, ds2.getFailureIndices());

        Index indexToAdd = randomFrom(ds2.getIndices().toArray(Index.EMPTY_ARRAY));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ds1.addBackingIndex(builder.build().getProject(), indexToAdd)
        );
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

    public void testAddBackingIndexThatIsPartOfDataStreamFailureStore() {
        Metadata.Builder builder = Metadata.builder();

        DataStream ds1 = createRandomDataStream();
        DataStream ds2 = createRandomDataStream();
        builder.put(ds1);
        builder.put(ds2);

        createMetadataForIndices(builder, ds1.getIndices());
        createMetadataForIndices(builder, ds1.getFailureIndices());
        createMetadataForIndices(builder, ds2.getIndices());
        createMetadataForIndices(builder, ds2.getFailureIndices());

        Index indexToAdd = randomFrom(ds2.getFailureIndices().toArray(Index.EMPTY_ARRAY));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ds1.addBackingIndex(builder.build().getProject(), indexToAdd)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] because it is already a failure store index on data stream [%s]",
                    indexToAdd.getName(),
                    ds1.getName(),
                    ds2.getName()
                )
            )
        );
    }

    public void testAddExistingBackingIndex() {
        Metadata.Builder builder = Metadata.builder();

        DataStream original = createRandomDataStream();
        builder.put(original);

        createMetadataForIndices(builder, original.getIndices());

        Index indexToAdd = randomFrom(original.getIndices().toArray(Index.EMPTY_ARRAY));

        DataStream updated = original.addBackingIndex(builder.build().getProject(), indexToAdd);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration()));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size()));
        for (int k = 0; k < original.getIndices().size(); k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k)));
        }
    }

    public void testAddBackingIndexWithAliases() {
        Metadata.Builder builder = Metadata.builder();

        DataStream original = createRandomDataStream();
        builder.put(original);

        createMetadataForIndices(builder, original.getIndices());

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
            () -> original.addBackingIndex(builder.build().getProject(), indexToAdd)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] until its %s [%s] %s removed",
                    indexToAdd.getName(),
                    original.getName(),
                    numAliases > 1 ? "aliases" : "alias",
                    Strings.arrayToCommaDelimitedString(aliasNames),
                    numAliases > 1 ? "are" : "is"
                )
            )
        );
    }

    public void testAddFailureStoreIndex() {
        Metadata.Builder builder = Metadata.builder();

        DataStream original = createRandomDataStream();
        builder.put(original);

        createMetadataForIndices(builder, original.getIndices());
        createMetadataForIndices(builder, original.getFailureIndices());

        Index indexToAdd = new Index(randomAlphaOfLength(4), UUIDs.randomBase64UUID(random()));
        builder.put(
            IndexMetadata.builder(indexToAdd.getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(),
            false
        );

        DataStream updated = original.addFailureStoreIndex(builder.build().getProject(), indexToAdd);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size()));
        assertThat(updated.getFailureIndices().size(), equalTo(original.getFailureIndices().size() + 1));
        for (int k = 1; k <= original.getFailureIndices().size(); k++) {
            assertThat(updated.getFailureIndices().get(k), equalTo(original.getFailureIndices().get(k - 1)));
        }
        assertThat(updated.getFailureIndices().get(0), equalTo(indexToAdd));
    }

    public void testAddFailureStoreIndexThatIsPartOfAnotherDataStream() {
        Metadata.Builder builder = Metadata.builder();

        DataStream ds1 = createRandomDataStream();
        DataStream ds2 = createRandomDataStream();
        builder.put(ds1);
        builder.put(ds2);

        createMetadataForIndices(builder, ds1.getIndices());
        createMetadataForIndices(builder, ds1.getFailureIndices());
        createMetadataForIndices(builder, ds2.getIndices());
        createMetadataForIndices(builder, ds2.getFailureIndices());

        Index indexToAdd = randomFrom(ds2.getFailureIndices().toArray(Index.EMPTY_ARRAY));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ds1.addFailureStoreIndex(builder.build().getProject(), indexToAdd)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] because it is already a failure store index on data stream [%s]",
                    indexToAdd.getName(),
                    ds1.getName(),
                    ds2.getName()
                )
            )
        );
    }

    public void testAddFailureStoreIndexThatIsPartOfDataStreamBackingIndices() {
        Metadata.Builder builder = Metadata.builder();

        DataStream ds1 = createRandomDataStream();
        DataStream ds2 = createRandomDataStream();
        builder.put(ds1);
        builder.put(ds2);

        createMetadataForIndices(builder, ds1.getIndices());
        createMetadataForIndices(builder, ds1.getFailureIndices());
        createMetadataForIndices(builder, ds2.getIndices());
        createMetadataForIndices(builder, ds2.getFailureIndices());

        Index indexToAdd = randomFrom(ds2.getIndices().toArray(Index.EMPTY_ARRAY));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ds1.addFailureStoreIndex(builder.build().getProject(), indexToAdd)
        );
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

    public void testAddExistingFailureStoreIndex() {
        Metadata.Builder builder = Metadata.builder();

        DataStream original = createRandomDataStream();
        builder.put(original);

        createMetadataForIndices(builder, original.getIndices());
        createMetadataForIndices(builder, original.getFailureIndices());

        Index indexToAdd = randomFrom(original.getFailureIndices().toArray(Index.EMPTY_ARRAY));

        DataStream updated = original.addFailureStoreIndex(builder.build().getProject(), indexToAdd);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration()));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size()));
        assertThat(updated.getFailureIndices().size(), equalTo(original.getFailureIndices().size()));
        assertThat(updated.getFailureIndices(), equalTo(original.getFailureIndices()));
    }

    public void testAddFailureStoreIndexWithAliases() {
        Metadata.Builder builder = Metadata.builder();

        DataStream original = createRandomDataStream();
        builder.put(original);

        createMetadataForIndices(builder, original.getIndices());
        createMetadataForIndices(builder, original.getFailureIndices());

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
            () -> original.addFailureStoreIndex(builder.build().getProject(), indexToAdd)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] until its %s [%s] %s removed",
                    indexToAdd.getName(),
                    original.getName(),
                    numAliases > 1 ? "aliases" : "alias",
                    Strings.arrayToCommaDelimitedString(aliasNames),
                    numAliases > 1 ? "are" : "is"
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
        DataStream original = createRandomDataStream();
        int indexToReplace = randomIntBetween(1, original.getIndices().size() - 1) - 1;

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        DataStream updated = original.replaceBackingIndex(original.getIndices().get(indexToReplace), newBackingIndex);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getIndices().size(), equalTo(original.getIndices().size()));
        assertThat(updated.getIndices().get(indexToReplace), equalTo(newBackingIndex));

        for (int i = 0; i < original.getIndices().size(); i++) {
            if (i != indexToReplace) {
                assertThat(updated.getIndices().get(i), equalTo(original.getIndices().get(i)));
            }
        }
    }

    public void testReplaceBackingIndexThrowsExceptionIfIndexNotPartOfDataStream() {
        DataStream original = createRandomDataStream();

        Index standaloneIndex = new Index("index-foo", UUIDs.randomBase64UUID(random()));
        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceBackingIndex(standaloneIndex, newBackingIndex));
    }

    public void testReplaceBackingIndexThrowsExceptionIfIndexPartOfFailureStore() {
        DataStream original = createRandomDataStream();
        int indexToReplace = randomIntBetween(1, original.getFailureIndices().size() - 1) - 1;

        Index failureIndex = original.getFailureIndices().get(indexToReplace);
        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceBackingIndex(failureIndex, newBackingIndex));
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

    public void testReplaceFailureIndex() {
        DataStream original = createRandomDataStream();
        int indexToReplace = randomIntBetween(1, original.getFailureIndices().size() - 1) - 1;

        Index newFailureIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        DataStream updated = original.replaceFailureStoreIndex(original.getFailureIndices().get(indexToReplace), newFailureIndex);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration() + 1));
        assertThat(updated.getFailureIndices().size(), equalTo(original.getFailureIndices().size()));
        assertThat(updated.getFailureIndices().get(indexToReplace), equalTo(newFailureIndex));

        for (int i = 0; i < original.getFailureIndices().size(); i++) {
            if (i != indexToReplace) {
                assertThat(updated.getFailureIndices().get(i), equalTo(original.getFailureIndices().get(i)));
            }
        }
    }

    public void testReplaceFailureIndexThrowsExceptionIfIndexNotPartOfDataStream() {
        DataStream original = createRandomDataStream();

        Index standaloneIndex = new Index("index-foo", UUIDs.randomBase64UUID(random()));
        Index newFailureIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceFailureStoreIndex(standaloneIndex, newFailureIndex));
    }

    public void testReplaceFailureIndexThrowsExceptionIfIndexPartOfBackingIndices() {
        DataStream original = createRandomDataStream();
        int indexToReplace = randomIntBetween(1, original.getIndices().size() - 1) - 1;

        Index backingIndex = original.getIndices().get(indexToReplace);
        Index newFailureIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceFailureStoreIndex(backingIndex, newFailureIndex));
    }

    public void testReplaceFailureIndexThrowsExceptionIfReplacingWriteIndex() {
        int numFailureIndices = randomIntBetween(2, 32);
        int writeIndexPosition = numFailureIndices - 1;
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        long ts = System.currentTimeMillis();

        List<Index> indices = new ArrayList<>(1);
        indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts), UUIDs.randomBase64UUID(random())));

        List<Index> failureIndices = new ArrayList<>(numFailureIndices);
        for (int i = 1; i <= numFailureIndices; i++) {
            failureIndices.add(new Index(DataStream.getDefaultFailureStoreName(dataStreamName, i, ts), UUIDs.randomBase64UUID(random())));
        }
        int generation = randomBoolean() ? numFailureIndices : numFailureIndices + randomIntBetween(1, 5);
        DataStream original = newInstance(dataStreamName, indices, generation, null, false, null, failureIndices);

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> original.replaceFailureStoreIndex(failureIndices.get(writeIndexPosition), newBackingIndex)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "cannot replace failure index [%s] of data stream [%s] because it is the failure store write index",
                    failureIndices.get(writeIndexPosition).getName(),
                    dataStreamName
                )
            )
        );
    }

    public void testSnapshot() {
        var preSnapshotDataStream = DataStreamTestHelper.randomInstance(true);

        // Mutate backing indices
        var backingIndicesToRemove = randomSubsetOf(preSnapshotDataStream.getIndices());
        if (backingIndicesToRemove.size() == preSnapshotDataStream.getIndices().size()) {
            // never remove them all
            backingIndicesToRemove.remove(0);
        }
        var backingIndicesToAdd = randomIndexInstances();
        var postSnapshotBackingIndices = new ArrayList<>(preSnapshotDataStream.getIndices());
        postSnapshotBackingIndices.removeAll(backingIndicesToRemove);
        postSnapshotBackingIndices.addAll(backingIndicesToAdd);

        // Mutate failure indices
        var failureIndicesToRemove = randomSubsetOf(preSnapshotDataStream.getFailureIndices());
        var failureIndicesToAdd = randomIndexInstances();
        var postSnapshotFailureIndices = new ArrayList<>(preSnapshotDataStream.getFailureIndices());
        postSnapshotFailureIndices.removeAll(failureIndicesToRemove);
        postSnapshotFailureIndices.addAll(failureIndicesToAdd);

        var replicated = preSnapshotDataStream.isReplicated() && randomBoolean();
        var postSnapshotDataStream = preSnapshotDataStream.copy()
            .setBackingIndices(
                preSnapshotDataStream.getDataComponent()
                    .copy()
                    .setIndices(postSnapshotBackingIndices)
                    .setRolloverOnWrite(replicated == false && preSnapshotDataStream.rolloverOnWrite())
                    .build()
            )
            .setFailureIndices(
                preSnapshotDataStream.getFailureComponent()
                    .copy()
                    .setIndices(postSnapshotFailureIndices)
                    .setRolloverOnWrite(replicated == false && preSnapshotDataStream.rolloverOnWrite())
                    .build()
            )
            .setGeneration(preSnapshotDataStream.getGeneration() + randomIntBetween(0, 5))
            .setMetadata(preSnapshotDataStream.getMetadata() == null ? null : new HashMap<>(preSnapshotDataStream.getMetadata()))
            .setReplicated(replicated)
            .build();

        Set<String> indicesInSnapshot = new HashSet<>();
        preSnapshotDataStream.getIndices().forEach(index -> indicesInSnapshot.add(index.getName()));
        preSnapshotDataStream.getFailureIndices().forEach(index -> indicesInSnapshot.add(index.getName()));
        var reconciledDataStream = postSnapshotDataStream.snapshot(indicesInSnapshot, Metadata.builder());

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
        assertThat(reconciledDataStream.getIndices(), everyItem(not(in(backingIndicesToRemove))));
        assertThat(reconciledDataStream.getIndices(), everyItem(not(in(backingIndicesToAdd))));
        assertThat(
            reconciledDataStream.getIndices().size(),
            equalTo(preSnapshotDataStream.getIndices().size() - backingIndicesToRemove.size())
        );
        var reconciledFailureIndices = reconciledDataStream.getFailureIndices();
        assertThat(reconciledFailureIndices, everyItem(not(in(failureIndicesToRemove))));
        assertThat(reconciledFailureIndices, everyItem(not(in(failureIndicesToAdd))));
        assertThat(
            reconciledFailureIndices.size(),
            equalTo(preSnapshotDataStream.getFailureIndices().size() - failureIndicesToRemove.size())
        );
    }

    public void testSnapshotWithAllBackingIndicesRemoved() {
        var preSnapshotDataStream = DataStreamTestHelper.randomInstance();
        var indicesToAdd = randomNonEmptyIndexInstances();

        var postSnapshotDataStream = preSnapshotDataStream.copy()
            .setBackingIndices(preSnapshotDataStream.getDataComponent().copy().setIndices(indicesToAdd).build())
            .build();

        assertNull(
            postSnapshotDataStream.snapshot(
                preSnapshotDataStream.getIndices().stream().map(Index::getName).collect(Collectors.toSet()),
                Metadata.builder()
            )
        );
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
        ProjectMetadata project = clusterState.getMetadata().getProject();

        DataStream dataStream = project.dataStreams().get(dataStreamName);
        Index result = dataStream.selectTimeSeriesWriteIndex(currentTime, project);
        assertThat(result, equalTo(dataStream.getIndices().get(1)));
        assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, start2.toEpochMilli())));

        result = dataStream.selectTimeSeriesWriteIndex(currentTime.minus(2, ChronoUnit.HOURS), project);
        assertThat(result, equalTo(dataStream.getIndices().get(1)));
        assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, start2.toEpochMilli())));

        result = dataStream.selectTimeSeriesWriteIndex(currentTime.minus(3, ChronoUnit.HOURS), project);
        assertThat(result, equalTo(dataStream.getIndices().get(0)));
        assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, start1.toEpochMilli())));

        result = dataStream.selectTimeSeriesWriteIndex(currentTime.minus(6, ChronoUnit.HOURS), project);
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
            DataStream dataStream = clusterState.getMetadata().getProject().dataStreams().get(dataStreamName);
            assertThat(dataStream, notNullValue());
            assertThat(dataStream.getIndices(), hasSize(2));
            assertThat(
                IndexSettings.TIME_SERIES_START_TIME.get(
                    clusterState.getMetadata().getProject().index(dataStream.getIndices().get(0)).getSettings()
                ),
                equalTo(start1)
            );
            assertThat(
                IndexSettings.TIME_SERIES_END_TIME.get(
                    clusterState.getMetadata().getProject().index(dataStream.getIndices().get(0)).getSettings()
                ),
                equalTo(end1)
            );
            assertThat(
                IndexSettings.TIME_SERIES_START_TIME.get(
                    clusterState.getMetadata().getProject().index(dataStream.getIndices().get(1)).getSettings()
                ),
                equalTo(start2)
            );
            assertThat(
                IndexSettings.TIME_SERIES_END_TIME.get(
                    clusterState.getMetadata().getProject().index(dataStream.getIndices().get(1)).getSettings()
                ),
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
            DataStream dataStream = clusterState.getMetadata().getProject().dataStreams().get(dataStreamName);

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
                            indexSettings(1, 1).put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current()).build()
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
                                indexSettings(1, 1).put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
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
            DataStream dataStream = createDataStream(dataStreamName, List.of(indexMetadata.getIndex()));
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
            DataStream dataStream = createDataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex())
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
            DataStream dataStream = createDataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex())
            );
            assertThat(dataStream.getGenerationLifecycleDate(indexMetadata).millis(), is(creationTimeMillis));
        }
        {
            // for a write index that has not been rolled over yet, we get null even if the index has an origination date
            long originTimeMillis = creationTimeMillis - 3000L;
            IndexMetadata.Builder backingIndexMetadataBuilder = IndexMetadata.builder(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1)
            )
                .settings(settings(IndexVersion.current()).put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);
            IndexMetadata backingIndexMetadata = backingIndexMetadataBuilder.build();
            IndexMetadata.Builder failureIndexMetadataBuilder = IndexMetadata.builder(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1)
            )
                .settings(settings(IndexVersion.current()).put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(creationTimeMillis);
            IndexMetadata failureIndexMetadata = failureIndexMetadataBuilder.build();
            DataStream dataStream = createDataStream(
                dataStreamName,
                List.of(backingIndexMetadata.getIndex()),
                List.of(failureIndexMetadata.getIndex())
            );

            assertNull(dataStream.getGenerationLifecycleDate(backingIndexMetadata));
            assertNull(dataStream.getGenerationLifecycleDate(failureIndexMetadata));
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
            DataStream dataStream = createDataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex())
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
            DataStream dataStream = createDataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex())
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
            DataStream dataStream = createDataStream(
                dataStreamName,
                List.of(indexMetadata.getIndex(), writeIndexMetaBuilder.build().getIndex())
            );
            assertThat(dataStream.getGenerationLifecycleDate(indexMetadata).millis(), is(originTimeMillis));
        }
    }

    private DataStream createDataStream(String name, List<Index> indices) {
        return DataStream.builder(name, indices)
            .setMetadata(Map.of())
            .setReplicated(randomBoolean())
            .setAllowCustomRouting(randomBoolean())
            .setIndexMode(IndexMode.STANDARD)
            .build();
    }

    private DataStream createDataStream(String name, List<Index> backingIndices, List<Index> failureIndices) {
        return DataStream.builder(name, backingIndices)
            .setMetadata(Map.of())
            .setReplicated(randomBoolean())
            .setAllowCustomRouting(randomBoolean())
            .setIndexMode(IndexMode.STANDARD)
            .setFailureIndices(DataStream.DataStreamIndices.failureIndicesBuilder(failureIndices).build())
            .build();
    }

    public void testGetBackingIndicesPastRetention() {
        testIndicesPastRetention(false);
    }

    public void testGetFailureIndicesPastRetention() {
        testIndicesPastRetention(true);
    }

    private void testIndicesPastRetention(boolean failureStore) {
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
            {
                // No lifecycle or disabled for data and only disabled lifecycle for failures should result in empty list.
                Metadata.Builder builder = Metadata.builder();
                var disabledLifecycle = DataStreamLifecycle.failuresLifecycleBuilder().enabled(false).build();
                DataStream dataStream = createDataStream(
                    builder,
                    dataStreamName,
                    creationAndRolloverTimes,
                    settings(IndexVersion.current()),
                    randomBoolean() ? DataStreamLifecycle.dataLifecycleBuilder().enabled(false).build() : null,
                    new DataStreamOptions(new DataStreamFailureStore(randomBoolean(), disabledLifecycle))
                );
                Metadata metadata = builder.build();

                assertThat(
                    dataStream.getIndicesPastRetention(metadata.getProject()::index, () -> now, TimeValue.ZERO, failureStore).isEmpty(),
                    is(true)
                );
            }
        }

        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            creationAndRolloverTimes,
            settings(IndexVersion.current()),
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
        );
        Metadata metadata = builder.build();
        Supplier<List<Index>> indicesSupplier = () -> failureStore ? dataStream.getFailureIndices() : dataStream.getIndices();

        {
            // Mix of indices younger and older than retention
            List<Index> indicesPastRetention = dataStream.getIndicesPastRetention(
                metadata.getProject()::index,
                () -> now,
                TimeValue.timeValueSeconds(2500),
                failureStore
            );
            assertThat(indicesPastRetention.size(), is(2));
            for (int i = 0; i < indicesPastRetention.size(); i++) {
                assertThat(indicesPastRetention.get(i).getName(), is(indicesSupplier.get().get(i).getName()));
            }
        }

        {
            // All indices past retention, but we keep the write index
            List<Index> indicesPastRetention = dataStream.getIndicesPastRetention(
                metadata.getProject()::index,
                () -> now,
                TimeValue.ZERO,
                failureStore
            );
            assertThat(indicesPastRetention.size(), is(4));
            for (int i = 0; i < indicesPastRetention.size(); i++) {
                assertThat(indicesPastRetention.get(i).getName(), is(indicesSupplier.get().get(i).getName()));
            }
        }

        {
            // All indices younger than retention
            List<Index> indicesPastRetention = dataStream.getIndicesPastRetention(
                metadata.getProject()::index,
                () -> now,
                TimeValue.timeValueSeconds(6000),
                failureStore
            );
            assertThat(indicesPastRetention.isEmpty(), is(true));
        }

        {
            // Test ILM managed indices are excluded
            Function<String, IndexMetadata> indexMetadataWithSomeLifecycleSupplier = indexName -> {
                IndexMetadata indexMetadata = metadata.getProject().index(indexName);
                if (indexName.endsWith("00003") || indexName.endsWith("00005")) {
                    return indexMetadata;
                }
                return IndexMetadata.builder(indexMetadata)
                    .settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_NAME, "some-policy").build())
                    .build();
            };
            List<Index> indicesPastRetention = dataStream.getIndicesPastRetention(
                indexMetadataWithSomeLifecycleSupplier,
                () -> now,
                TimeValue.ZERO,
                failureStore
            );
            assertThat(indicesPastRetention.size(), is(1));
            assertThat(indicesPastRetention.get(0).getName(), is(indicesSupplier.get().get(2).getName()));
        }
    }

    public void testBackingIndicesPastRetentionWithOriginationDate() {
        testIndicesPastRetentionWithOriginationDate(false);
    }

    public void testFailureIndicesPastRetentionWithOriginationDate() {
        testIndicesPastRetentionWithOriginationDate(true);
    }

    private void testIndicesPastRetentionWithOriginationDate(boolean failureStore) {
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
        DataStream dataStream = createDataStream(
            metadataBuilder,
            dataStreamName,
            creationAndRolloverTimes,
            settings(IndexVersion.current()),
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
        );
        Metadata metadata = metadataBuilder.build();
        Supplier<List<Index>> indicesSupplier = () -> failureStore ? dataStream.getFailureIndices() : dataStream.getIndices();
        {
            // no retention configured so we expect an empty list
            assertThat(dataStream.getIndicesPastRetention(metadata.getProject()::index, () -> now, null, failureStore).isEmpty(), is(true));
        }

        {
            // retention period where first and second index is too old, and 5th has old origination date.
            List<Index> indicesPastRetention = dataStream.getIndicesPastRetention(
                metadata.getProject()::index,
                () -> now,
                TimeValue.timeValueMillis(2500),
                failureStore
            );
            assertThat(indicesPastRetention.size(), is(3));
            assertThat(indicesPastRetention.get(0).getName(), is(indicesSupplier.get().get(0).getName()));
            assertThat(indicesPastRetention.get(1).getName(), is(indicesSupplier.get().get(1).getName()));
            assertThat(indicesPastRetention.get(2).getName(), is(indicesSupplier.get().get(5).getName()));
        }

        {
            // no index matches the retention age
            List<Index> indicesPastRetention = dataStream.getIndicesPastRetention(
                metadata.getProject()::index,
                () -> now,
                TimeValue.timeValueMillis(9000),
                failureStore
            );
            assertThat(indicesPastRetention.isEmpty(), is(true));
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
                DataStreamLifecycle.dataLifecycleBuilder()
                    .downsampling(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueMillis(2000),
                                new DownsampleConfig(new DateHistogramInterval("10m"))
                            ),
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueMillis(3200),
                                new DownsampleConfig(new DateHistogramInterval("100m"))
                            ),
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueMillis(3500),
                                new DownsampleConfig(new DateHistogramInterval("1000m"))

                            )
                        )
                    )
                    .build()
            );
            Metadata metadata = builder.build();

            // generation time is now - 2000
            String thirdGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 3);
            Index thirdIndex = metadata.getProject().index(thirdGeneration).getIndex();
            List<DataStreamLifecycle.DownsamplingRound> roundsForThirdIndex = dataStream.getDownsamplingRoundsFor(
                thirdIndex,
                metadata.getProject()::index,
                () -> now
            );

            assertThat(roundsForThirdIndex.size(), is(1));
            assertThat(roundsForThirdIndex.get(0).after(), is(TimeValue.timeValueMillis(2000)));

            // generation time is now - 40000
            String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            Index firstIndex = metadata.getProject().index(firstGeneration).getIndex();
            List<DataStreamLifecycle.DownsamplingRound> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata.getProject()::index,
                () -> now
            );
            // expecting all rounds to match this index
            assertThat(roundsForFirstIndex.size(), is(3));
            // assert the order is maintained
            assertThat(
                roundsForFirstIndex.stream().map(DataStreamLifecycle.DownsamplingRound::after).toList(),
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
                DataStreamLifecycle.dataLifecycleBuilder()
                    .downsampling(
                        List.of(
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueMillis(2000),
                                new DownsampleConfig(new DateHistogramInterval("10m"))
                            ),
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueMillis(3200),
                                new DownsampleConfig(new DateHistogramInterval("100m"))
                            ),
                            new DataStreamLifecycle.DownsamplingRound(
                                TimeValue.timeValueMillis(3500),
                                new DownsampleConfig(new DateHistogramInterval("1000m"))
                            )
                        )

                    )
                    .build()
            );
            Metadata metadata = builder.build();

            // generation time is now - 40000
            String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            Index firstIndex = metadata.getProject().index(firstGeneration).getIndex();
            List<DataStreamLifecycle.DownsamplingRound> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata.getProject()::index,
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
            Index firstIndex = metadata.getProject().index(firstGeneration).getIndex();
            List<DataStreamLifecycle.DownsamplingRound> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata.getProject()::index,
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
                DataStreamLifecycle.dataLifecycleBuilder().build()
            );
            Metadata metadata = builder.build();

            String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            Index firstIndex = metadata.getProject().index(firstGeneration).getIndex();
            List<DataStreamLifecycle.DownsamplingRound> roundsForFirstIndex = dataStream.getDownsamplingRoundsFor(
                firstIndex,
                metadata.getProject()::index,
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
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).build()
        );
        Metadata metadata = builder.build();

        {
            // false for indices not part of the data stream
            assertThat(
                dataStream.isIndexManagedByDataStreamLifecycle(new Index("standalone_index", "uuid"), metadata.getProject()::index),
                is(false)
            );
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
                unmanagedDataStream.isIndexManagedByDataStreamLifecycle(
                    unmanagedDataStream.getIndices().get(1),
                    newMetadata.getProject()::index
                ),
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
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
            );
            Metadata metadataIlm = builderWithIlm.build();
            for (Index index : ds.getIndices()) {
                assertThat(ds.isIndexManagedByDataStreamLifecycle(index, metadataIlm.getProject()::index), is(false));
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
                    DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
                );
                Metadata metadataIlm = builderWithIlm.build();
                for (Index index : ds.getIndices()) {
                    assertThat(ds.isIndexManagedByDataStreamLifecycle(index, metadataIlm.getProject()::index), is(true));
                }
            }
        }

        {
            // true otherwise
            for (Index index : dataStream.getIndices()) {
                assertThat(dataStream.isIndexManagedByDataStreamLifecycle(index, metadata.getProject()::index), is(true));
            }
        }
    }

    public void testFailuresLifecycle() {
        DataStream noFailureStoreDs = DataStream.builder("no-fs", List.of(new Index(randomAlphaOfLength(10), randomUUID()))).build();
        assertThat(noFailureStoreDs.getFailuresLifecycle(), nullValue());

        assertThat(noFailureStoreDs.getFailuresLifecycle(true), equalTo(DataStreamLifecycle.DEFAULT_FAILURE_LIFECYCLE));
        assertThat(noFailureStoreDs.getFailuresLifecycle(randomBoolean() ? false : null), nullValue());

        DataStream withFailureIndices = DataStream.builder("with-fs-indices", List.of(new Index(randomAlphaOfLength(10), randomUUID())))
            .setFailureIndices(
                DataStream.DataStreamIndices.failureIndicesBuilder(List.of(new Index(randomAlphaOfLength(10), randomUUID()))).build()
            )
            .build();
        assertThat(withFailureIndices.getFailuresLifecycle(), equalTo(DataStreamLifecycle.DEFAULT_FAILURE_LIFECYCLE));

        DataStreamLifecycle lifecycle = DataStreamLifecycleTests.randomFailuresLifecycle();
        DataStream withFailuresLifecycle = DataStream.builder("with-fs", List.of(new Index(randomAlphaOfLength(10), randomUUID())))
            .setDataStreamOptions(new DataStreamOptions(new DataStreamFailureStore(randomBoolean(), lifecycle)))
            .build();
        assertThat(withFailuresLifecycle.getFailuresLifecycle(), equalTo(lifecycle));
    }

    private DataStream createDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        List<DataStreamMetadata> creationAndRolloverTimes,
        Settings.Builder backingIndicesSettings,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        return createDataStream(builder, dataStreamName, creationAndRolloverTimes, backingIndicesSettings, lifecycle, null);
    }

    private DataStream createDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        List<DataStreamMetadata> creationAndRolloverTimes,
        Settings.Builder backingIndicesSettings,
        @Nullable DataStreamLifecycle lifecycle,
        @Nullable DataStreamOptions dataStreamOptions
    ) {
        int backingIndicesCount = creationAndRolloverTimes.size();
        final List<Index> backingIndices = createDataStreamIndices(
            builder,
            dataStreamName,
            creationAndRolloverTimes,
            backingIndicesSettings,
            backingIndicesCount,
            false
        );
        final List<Index> failureIndices = createDataStreamIndices(
            builder,
            dataStreamName,
            creationAndRolloverTimes,
            backingIndicesSettings,
            backingIndicesCount,
            true
        );
        return newInstance(dataStreamName, backingIndices, backingIndicesCount, null, false, lifecycle, failureIndices, dataStreamOptions);
    }

    private static List<Index> createDataStreamIndices(
        Metadata.Builder builder,
        String dataStreamName,
        List<DataStreamMetadata> creationAndRolloverTimes,
        Settings.Builder backingIndicesSettings,
        int backingIndicesCount,
        boolean isFailureStore
    ) {
        List<Index> indices = new ArrayList<>(backingIndicesCount);
        for (int k = 1; k <= backingIndicesCount; k++) {
            DataStreamMetadata creationRolloverTime = creationAndRolloverTimes.get(k - 1);
            String indexName = isFailureStore
                ? getDefaultFailureStoreName(dataStreamName, k, System.currentTimeMillis())
                : DataStream.getDefaultBackingIndexName(dataStreamName, k);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexName)
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
            } else {
                // We reuse the backingIndicesSettings, so it's important to reset it
                backingIndicesSettings.putNull(LIFECYCLE_ORIGINATION_DATE);
            }
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            indices.add(indexMetadata.getIndex());
        }
        return indices;
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
        var failuresLifecycle = randomBoolean() ? null : DataStreamLifecycleTests.randomFailuresLifecycle();

        DataStreamLifecycle lifecycle = DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE;
        boolean isSystem = randomBoolean();
        DataStream dataStream = new DataStream(
            dataStreamName,
            indices,
            generation,
            metadata,
            randomSettings(),
            isSystem,
            randomBoolean(),
            isSystem,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : null, // IndexMode.TIME_SERIES triggers validation that many unit tests doesn't pass
            lifecycle,
            new DataStreamOptions(new DataStreamFailureStore(failureStore, failuresLifecycle)),
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
            for (String label : rolloverConfiguration.resolveRolloverConditions(
                lifecycle.getEffectiveDataRetention(globalRetention, dataStream.isInternal())
            ).getConditions().keySet()) {
                assertThat(serialized, containsString(label));
            }
            if (dataStream.isInternal() == false
                && (globalRetention.defaultRetention() != null || globalRetention.maxRetention() != null)) {
                assertThat(serialized, containsString("effective_retention"));
            } else {
                assertThat(serialized, not(containsString("effective_retention")));
            }

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

        final DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(backingIndices.size())
            .setMetadata(Map.of())
            .setIndexMode(randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES)
            .build();

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

        final DataStream dataStream = DataStream.builder(dataStreamName, backingIndices)
            .setGeneration(backingIndices.size())
            .setMetadata(Map.of())
            .setIndexMode(randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES)
            .build();

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
            builder.withShardWriteLoad(
                shardId,
                randomDoubleBetween(0, 64, true),
                randomDoubleBetween(0, 64, true),
                randomDoubleBetween(0, 64, true),
                randomLongBetween(1, 10)
            );
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
        boolean replicated = randomBoolean();
        DataStream noFailureStoreDataStream = new DataStream(
            randomAlphaOfLength(10),
            randomNonEmptyIndexInstances(),
            randomNonNegativeInt(),
            null,
            randomSettings(),
            hidden,
            replicated,
            system,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomDataLifecycle(),
            DataStreamOptions.FAILURE_STORE_DISABLED,
            List.of(),
            replicated == false && randomBoolean(),
            null
        );
        assertThat(noFailureStoreDataStream.getWriteFailureIndex(), nullValue());

        DataStream failureStoreDataStreamWithEmptyFailureIndices = new DataStream(
            randomAlphaOfLength(10),
            randomNonEmptyIndexInstances(),
            randomNonNegativeInt(),
            null,
            randomSettings(),
            hidden,
            replicated,
            system,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomDataLifecycle(),
            DataStreamOptions.FAILURE_STORE_ENABLED,
            List.of(),
            replicated == false && randomBoolean(),
            null
        );
        assertThat(failureStoreDataStreamWithEmptyFailureIndices.getWriteFailureIndex(), nullValue());

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
            randomSettings(),
            hidden,
            replicated,
            system,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomDataLifecycle(),
            DataStreamOptions.FAILURE_STORE_ENABLED,
            failureIndices,
            replicated == false && randomBoolean(),
            null
        );
        assertThat(failureStoreDataStream.getWriteFailureIndex(), is(writeFailureIndex));
    }

    public void testIsFailureIndex() {
        boolean hidden = randomBoolean();
        boolean system = hidden && randomBoolean();
        List<Index> backingIndices = randomNonEmptyIndexInstances();
        boolean replicated = randomBoolean();
        DataStream noFailureStoreDataStream = new DataStream(
            randomAlphaOfLength(10),
            backingIndices,
            randomNonNegativeInt(),
            null,
            randomSettings(),
            hidden,
            replicated,
            system,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomDataLifecycle(),
            DataStreamOptions.FAILURE_STORE_DISABLED,
            List.of(),
            replicated == false && randomBoolean(),
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
            randomSettings(),
            hidden,
            replicated,
            system,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomDataLifecycle(),
            DataStreamOptions.FAILURE_STORE_ENABLED,
            List.of(),
            replicated == false && randomBoolean(),
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
            randomSettings(),
            hidden,
            replicated,
            system,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : IndexMode.TIME_SERIES,
            DataStreamLifecycleTests.randomDataLifecycle(),
            DataStreamOptions.FAILURE_STORE_ENABLED,
            failureIndices,
            replicated == false && randomBoolean(),
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

    public void testInternalDataStream() {
        var nonInternalDataStream = createTestInstance().copy().setSystem(false).setName(randomAlphaOfLength(10)).build();
        assertThat(nonInternalDataStream.isInternal(), is(false));

        var systemDataStream = nonInternalDataStream.copy().setSystem(true).setHidden(true).setName(randomAlphaOfLength(10)).build();
        assertThat(systemDataStream.isInternal(), is(true));

        var dotPrefixedDataStream = nonInternalDataStream.copy().setSystem(false).setName("." + randomAlphaOfLength(10)).build();
        assertThat(dotPrefixedDataStream.isInternal(), is(true));
    }

    public void testIsFailureStoreExplicitlyEnabled() {
        DataStream dataStreamNoFailureStoreOptions = createTestInstance().copy().setDataStreamOptions(DataStreamOptions.EMPTY).build();
        DataStream dataStreamFailureStoreDisabled = createTestInstance().copy()
            .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_DISABLED)
            .build();
        DataStream dataStreamFailureStoreEnabled = createTestInstance().copy()
            .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_ENABLED)
            .build();
        assertThat(dataStreamNoFailureStoreOptions.isFailureStoreExplicitlyEnabled(), is(false));
        assertThat(dataStreamFailureStoreDisabled.isFailureStoreExplicitlyEnabled(), is(false));
        assertThat(dataStreamFailureStoreEnabled.isFailureStoreExplicitlyEnabled(), is(true));
    }

    public void testIsFailureStoreEffectivelyEnabled_instanceMethod() {
        DataStream dataStreamNoFailureStoreOptions = createTestInstance().copy()
            .setName("my-data-stream-no-failure-store-options")
            .setDataStreamOptions(DataStreamOptions.EMPTY)
            .build();
        DataStream dataStreamFailureStoreExplicitlyDisabled = createTestInstance().copy()
            .setName("my-data-stream-failure-store-explicitly-disabled")
            .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_DISABLED)
            .build();
        DataStream dataStreamFailureStoreExplicitlyEnabled = createTestInstance().copy()
            .setName("my-data-stream-failure-store-explicitly-enabled")
            .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_ENABLED)
            .build();
        DataStream dotPrefixDataStreamNoFailureStoreOptions = createTestInstance().copy()
            .setName(".my-data-stream-no-failure-store-options")
            .setDataStreamOptions(DataStreamOptions.EMPTY)
            .build();
        DataStream systemDataStreamNoFailureStoreOptions = createTestInstance().copy()
            .setName("my-data-stream-system-no-failure-store-options")
            .setDataStreamOptions(DataStreamOptions.EMPTY)
            .setSystem(true)
            .setHidden(true) // system indices must be hidden
            .build();
        DataStreamFailureStoreSettings matchingSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(
                        DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(),
                        String.join(",", "my-data-stream-*", ".my-data-stream-*")
                    )
                    .build()
            )
        );
        DataStreamFailureStoreSettings nonMatchingSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "not-my-data-stream-*")
                    .build()
            )
        );
        assertThat(dataStreamNoFailureStoreOptions.isFailureStoreEffectivelyEnabled(matchingSettings), is(true));
        assertThat(dataStreamNoFailureStoreOptions.isFailureStoreEffectivelyEnabled(nonMatchingSettings), is(false));
        assertThat(dataStreamFailureStoreExplicitlyDisabled.isFailureStoreEffectivelyEnabled(matchingSettings), is(false));
        assertThat(dataStreamFailureStoreExplicitlyDisabled.isFailureStoreEffectivelyEnabled(nonMatchingSettings), is(false));
        assertThat(dataStreamFailureStoreExplicitlyEnabled.isFailureStoreEffectivelyEnabled(matchingSettings), is(true));
        assertThat(dataStreamFailureStoreExplicitlyEnabled.isFailureStoreEffectivelyEnabled(nonMatchingSettings), is(true));
        assertThat(dotPrefixDataStreamNoFailureStoreOptions.isFailureStoreEffectivelyEnabled(matchingSettings), is(false));
        assertThat(dotPrefixDataStreamNoFailureStoreOptions.isFailureStoreEffectivelyEnabled(nonMatchingSettings), is(false));
        assertThat(systemDataStreamNoFailureStoreOptions.isFailureStoreEffectivelyEnabled(matchingSettings), is(false));
        assertThat(systemDataStreamNoFailureStoreOptions.isFailureStoreEffectivelyEnabled(nonMatchingSettings), is(false));
    }

    public void testIsFailureStoreEffectivelyEnabled_staticHelperMethod() {
        String regularDataStreamName = "my-data-stream";
        String dotPrefixedDataStreamName = ".my-dot-prefixed-data-stream";
        String systemDataStreamName = "my-system-data-stream-name";
        DataStreamFailureStoreSettings matchingSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(
                        DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(),
                        String.join(",", regularDataStreamName, dotPrefixedDataStreamName, systemDataStreamName)
                    )
                    .build()
            )
        );
        DataStreamFailureStoreSettings nonMatchingSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "not-my-data-stream")
                    .build()
            )
        );
        // At time of writing, SystemDataStreamDescriptor does not allow us to declare system data streams which aren't also dot-prefixed.
        // But we code defensively to do the system data stream and dot-prefix tests independently, as implied in the requirements.
        // We use a mock SystemIndices instance for testing, so that we can make it treat a non-dot-prefixed name as a system data stream.
        SystemIndices systemIndices = mock(SystemIndices.class);
        when(systemIndices.isSystemDataStream(systemDataStreamName)).thenReturn(true);

        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(DataStreamOptions.EMPTY, matchingSettings, regularDataStreamName, systemIndices),
            is(true)
        );
        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(DataStreamOptions.EMPTY, nonMatchingSettings, regularDataStreamName, systemIndices),
            is(false)
        );
        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(
                DataStreamOptions.EMPTY,
                matchingSettings,
                dotPrefixedDataStreamName,
                systemIndices
            ),
            is(false)
        );
        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(DataStreamOptions.EMPTY, matchingSettings, systemDataStreamName, systemIndices),
            is(false)
        );
        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(
                DataStreamOptions.FAILURE_STORE_DISABLED,
                matchingSettings,
                regularDataStreamName,
                systemIndices
            ),
            is(false)
        );
        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(
                DataStreamOptions.FAILURE_STORE_DISABLED,
                nonMatchingSettings,
                regularDataStreamName,
                systemIndices
            ),
            is(false)
        );
        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(
                DataStreamOptions.FAILURE_STORE_ENABLED,
                matchingSettings,
                regularDataStreamName,
                systemIndices
            ),
            is(true)
        );
        assertThat(
            DataStream.isFailureStoreEffectivelyEnabled(
                DataStreamOptions.FAILURE_STORE_ENABLED,
                nonMatchingSettings,
                regularDataStreamName,
                systemIndices
            ),
            is(true)
        );
    }

    public void testGetEffectiveSettingsNoMatchingTemplate() {
        // No matching template, so we expect an IllegalArgumentException
        DataStream dataStream = createTestInstance();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        assertThrows(IllegalArgumentException.class, () -> dataStream.getEffectiveSettings(projectMetadataBuilder.build()));
    }

    public void testGetEffectiveSettingsTemplateSettingsOnly() {
        // We only have settings from the template, so we expect to get those back
        DataStream dataStream = createDataStream(Settings.EMPTY);
        Settings templateSettings = randomSettings();
        Template.Builder templateBuilder = Template.builder().settings(templateSettings);
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(templateBuilder)
            .build();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(Map.of(dataStream.getName(), indexTemplate));
        assertThat(dataStream.getEffectiveSettings(projectMetadataBuilder.build()), equalTo(templateSettings));
    }

    public void testGetEffectiveSettingsComponentTemplateSettingsOnly() {
        // We only have settings from a component template, so we expect to get those back
        DataStream dataStream = createDataStream(Settings.EMPTY);
        Settings templateSettings = Settings.EMPTY;
        Template.Builder indexTemplateBuilder = Template.builder().settings(templateSettings);
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(indexTemplateBuilder)
            .componentTemplates(List.of("component-template-1"))
            .build();
        Settings componentSettings = randomSettings();
        Template.Builder componentTemplateBuilder = Template.builder().settings(componentSettings);
        ComponentTemplate componentTemplate1 = new ComponentTemplate(componentTemplateBuilder.build(), null, null, null);
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(Map.of(dataStream.getName(), indexTemplate))
            .componentTemplates(Map.of("component-template-1", componentTemplate1));
        assertThat(dataStream.getEffectiveSettings(projectMetadataBuilder.build()), equalTo(componentSettings));
    }

    public void testGetEffectiveSettingsDataStreamSettingsOnly() {
        // We only have settings from the data stream, so we expect to get those back
        Settings dataStreamSettings = randomSettings();
        DataStream dataStream = createDataStream(dataStreamSettings);
        Settings templateSettings = Settings.EMPTY;
        Template.Builder templateBuilder = Template.builder().settings(templateSettings);
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(templateBuilder)
            .build();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(Map.of(dataStream.getName(), indexTemplate));
        assertThat(dataStream.getEffectiveSettings(projectMetadataBuilder.build()), equalTo(dataStreamSettings));
    }

    public void testGetEffectiveSettings() {
        // Here we have settings from both the template and the data stream, so we expect the data stream settings to take precedence
        Settings dataStreamSettings = Settings.builder()
            .put("index.setting1", "dataStreamValue")
            .put("index.setting2", "dataStreamValue")
            .put("index.setting3", (String) null) // This one gets removed from the effective settings
            .build();
        DataStream dataStream = createDataStream(dataStreamSettings);
        Settings templateSettings = Settings.builder()
            .put("index.setting1", "templateValue")
            .put("index.setting3", "templateValue")
            .put("index.setting4", "templateValue")
            .build();
        Template.Builder templateBuilder = Template.builder().settings(templateSettings);
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(templateBuilder)
            .build();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(Map.of(dataStream.getName(), indexTemplate));
        Settings mergedSettings = Settings.builder()
            .put("index.setting1", "dataStreamValue")
            .put("index.setting2", "dataStreamValue")
            .put("index.setting4", "templateValue")
            .build();
        assertThat(dataStream.getEffectiveSettings(projectMetadataBuilder.build()), equalTo(mergedSettings));
    }

    public void testGetEffectiveIndexTemplateNoMatchingTemplate() {
        // No matching template, so we expect an IllegalArgumentException
        DataStream dataStream = createTestInstance();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        assertThrows(IllegalArgumentException.class, () -> dataStream.getEffectiveIndexTemplate(projectMetadataBuilder.build()));
    }

    public void testGetEffectiveIndexTemplateTemplateSettingsOnly() {
        // We only have settings from the template, so the effective template will just be the original template
        DataStream dataStream = createDataStream(Settings.EMPTY);
        Settings templateSettings = randomSettings();
        Template.Builder templateBuilder = Template.builder().settings(templateSettings).mappings(randomMappings());
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(templateBuilder)
            .build();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(Map.of(dataStream.getName(), indexTemplate));
        assertThat(dataStream.getEffectiveIndexTemplate(projectMetadataBuilder.build()), equalTo(indexTemplate));
    }

    public void testGetEffectiveIndexTemplateDataStreamSettingsOnly() {
        // We only have settings from the data stream, so we expect to get only those back in the effective template
        Settings dataStreamSettings = randomSettings();
        DataStream dataStream = createDataStream(dataStreamSettings);
        Settings templateSettings = Settings.EMPTY;
        CompressedXContent templateMappings = randomMappings();
        Template.Builder templateBuilder = Template.builder().settings(templateSettings).mappings(templateMappings);
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(templateBuilder)
            .build();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(Map.of(dataStream.getName(), indexTemplate));
        Template.Builder expectedTemplateBuilder = Template.builder().settings(dataStreamSettings).mappings(templateMappings);
        ComposableIndexTemplate expectedEffectiveTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(expectedTemplateBuilder)
            .build();
        assertThat(dataStream.getEffectiveIndexTemplate(projectMetadataBuilder.build()), equalTo(expectedEffectiveTemplate));
    }

    public void testGetEffectiveIndexTemplate() {
        // Here we have settings from both the template and the data stream, so we expect the data stream settings to take precedence
        Settings dataStreamSettings = Settings.builder()
            .put("index.setting1", "dataStreamValue")
            .put("index.setting2", "dataStreamValue")
            .put("index.setting3", (String) null) // This one gets removed from the effective settings
            .build();
        DataStream dataStream = createDataStream(dataStreamSettings);
        Settings templateSettings = Settings.builder()
            .put("index.setting1", "templateValue")
            .put("index.setting3", "templateValue")
            .put("index.setting4", "templateValue")
            .build();
        CompressedXContent templateMappings = randomMappings();
        Template.Builder templateBuilder = Template.builder().settings(templateSettings).mappings(templateMappings);
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(templateBuilder)
            .build();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(Map.of(dataStream.getName(), indexTemplate));
        Settings mergedSettings = Settings.builder()
            .put("index.setting1", "dataStreamValue")
            .put("index.setting2", "dataStreamValue")
            .put("index.setting4", "templateValue")
            .build();
        Template.Builder expectedTemplateBuilder = Template.builder().settings(mergedSettings).mappings(templateMappings);
        ComposableIndexTemplate expectedEffectiveTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName()))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(expectedTemplateBuilder)
            .build();
        assertThat(dataStream.getEffectiveIndexTemplate(projectMetadataBuilder.build()), equalTo(expectedEffectiveTemplate));
    }

    private DataStream createDataStream(Settings settings) {
        DataStream dataStream = createTestInstance();
        return dataStream.copy().setSettings(settings).build();
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

    private static DataStream createRandomDataStream() {
        long currentTimeMillis = System.currentTimeMillis();
        int numBackingIndices = randomIntBetween(2, 32);
        int numFailureIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k), UUIDs.randomBase64UUID(random())));
        }
        List<Index> failureIndices = new ArrayList<>(numFailureIndices);
        for (int k = 1; k <= numFailureIndices; k++) {
            failureIndices.add(
                new Index(DataStream.getDefaultFailureStoreName(dataStreamName, k, currentTimeMillis), UUIDs.randomBase64UUID(random()))
            );
        }
        return DataStreamTestHelper.newInstance(dataStreamName, indices, failureIndices);
    }

    private static void createMetadataForIndices(Metadata.Builder builder, List<Index> indices) {
        for (Index index : indices) {
            IndexMetadata im = IndexMetadata.builder(index.getName())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            builder.put(im, false);
        }
    }
}
