/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.Downsampling.Round;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.IndexSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.elasticsearch.index.IndexSettings.PREFER_ILM_SETTING;

public final class DataStream implements SimpleDiffable<DataStream>, ToXContentObject, IndexAbstraction {

    public static final FeatureFlag FAILURE_STORE_FEATURE_FLAG = new FeatureFlag("failure_store");
    public static final TransportVersion ADDED_FAILURE_STORE_TRANSPORT_VERSION = TransportVersions.V_8_12_0;
    public static final TransportVersion ADDED_AUTO_SHARDING_EVENT_VERSION = TransportVersions.DATA_STREAM_AUTO_SHARDING_EVENT;

    public static boolean isFailureStoreFeatureFlagEnabled() {
        return FAILURE_STORE_FEATURE_FLAG.isEnabled();
    }

    public static final String BACKING_INDEX_PREFIX = ".ds-";
    public static final String FAILURE_STORE_PREFIX = ".fs-";
    public static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");
    public static final String TIMESTAMP_FIELD_NAME = "@timestamp";
    // Timeseries indices' leaf readers should be sorted by desc order of their timestamp field, as it allows search time optimizations
    public static Comparator<LeafReader> TIMESERIES_LEAF_READERS_SORTER = Comparator.comparingLong((LeafReader r) -> {
        try {
            PointValues points = r.getPointValues(TIMESTAMP_FIELD_NAME);
            if (points != null) {
                byte[] sortValue = points.getMaxPackedValue();
                return LongPoint.decodeDimension(sortValue, 0);
            } else {
                // As we apply this segment sorter to any timeseries indices,
                // we don't have a guarantee that all docs contain @timestamp field.
                // Some segments may have all docs without @timestamp field, in this
                // case they will be sorted last.
                return Long.MIN_VALUE;
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Can't access [" + TIMESTAMP_FIELD_NAME + "] field for the index!", e);
        }
    }).reversed();

    private final LongSupplier timeProvider;
    private final String name;
    private final List<Index> indices;
    private final long generation;
    @Nullable
    private final Map<String, Object> metadata;
    private final boolean hidden;
    private final boolean replicated;
    private final boolean system;
    private final boolean allowCustomRouting;
    @Nullable
    private final IndexMode indexMode;
    @Nullable
    private final DataStreamLifecycle lifecycle;
    private final boolean rolloverOnWrite;
    private final boolean failureStoreEnabled;
    private final List<Index> failureIndices;
    private volatile Set<String> failureStoreLookup;
    @Nullable
    private final DataStreamAutoShardingEvent autoShardingEvent;

    public DataStream(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean hidden,
        boolean replicated,
        boolean system,
        boolean allowCustomRouting,
        IndexMode indexMode,
        DataStreamLifecycle lifecycle,
        boolean failureStoreEnabled,
        List<Index> failureIndices,
        boolean rolloverOnWrite,
        @Nullable DataStreamAutoShardingEvent autoShardingEvent
    ) {
        this(
            name,
            indices,
            generation,
            metadata,
            hidden,
            replicated,
            system,
            System::currentTimeMillis,
            allowCustomRouting,
            indexMode,
            lifecycle,
            failureStoreEnabled,
            failureIndices,
            rolloverOnWrite,
            autoShardingEvent
        );
    }

    // visible for testing
    DataStream(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean hidden,
        boolean replicated,
        boolean system,
        LongSupplier timeProvider,
        boolean allowCustomRouting,
        IndexMode indexMode,
        DataStreamLifecycle lifecycle,
        boolean failureStoreEnabled,
        List<Index> failureIndices,
        boolean rolloverOnWrite,
        @Nullable DataStreamAutoShardingEvent autoShardingEvent
    ) {
        this.name = name;
        this.indices = List.copyOf(indices);
        assert indices.isEmpty() == false;
        this.generation = generation;
        this.metadata = metadata;
        assert system == false || hidden; // system indices must be hidden
        this.hidden = hidden;
        this.replicated = replicated;
        this.timeProvider = timeProvider;
        this.system = system;
        this.allowCustomRouting = allowCustomRouting;
        this.indexMode = indexMode;
        this.lifecycle = lifecycle;
        this.failureStoreEnabled = failureStoreEnabled;
        this.failureIndices = failureIndices;
        assert assertConsistent(this.indices);
        assert replicated == false || rolloverOnWrite == false : "replicated data streams cannot be marked for lazy rollover";
        this.rolloverOnWrite = rolloverOnWrite;
        this.autoShardingEvent = autoShardingEvent;
    }

    private static boolean assertConsistent(List<Index> indices) {
        assert indices.size() > 0;
        final Set<String> indexNames = new HashSet<>();
        for (Index index : indices) {
            final boolean added = indexNames.add(index.getName());
            assert added : "found duplicate index entries in " + indices;
        }
        return true;
    }

    @Override
    public Type getType() {
        return Type.DATA_STREAM;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isDataStreamRelated() {
        return true;
    }

    @Override
    public List<Index> getIndices() {
        return indices;
    }

    public long getGeneration() {
        return generation;
    }

    public List<Index> getFailureIndices() {
        return failureIndices;
    }

    @Override
    public Index getWriteIndex() {
        return indices.get(indices.size() - 1);
    }

    /**
     * @return the write failure index if the failure store is enabled and there is already at least one failure, null otherwise
     */
    @Nullable
    public Index getFailureStoreWriteIndex() {
        return isFailureStoreEnabled() == false || failureIndices.isEmpty() ? null : failureIndices.get(failureIndices.size() - 1);
    }

    /**
     * Returns true if the index name provided belongs to a failure store index.
     * This method builds a local Set with all the failure store index names and then checks if it contains the name.
     * This will perform better if there are multiple indices of this data stream checked.
     */
    public boolean isFailureStoreIndex(String indexName) {
        if (failureStoreLookup == null) {
            // There is a chance this will be calculated twice, but it's a relatively cheap action,
            // so it's not worth synchronising
            if (failureIndices == null || failureIndices.isEmpty()) {
                failureStoreLookup = Set.of();
            } else {
                failureStoreLookup = failureIndices.stream().map(Index::getName).collect(Collectors.toSet());
            }
        }
        return failureStoreLookup.contains(indexName);
    }

    public boolean rolloverOnWrite() {
        return rolloverOnWrite;
    }

    /**
     * @param timestamp The timestamp used to select a backing index based on its start and end time.
     * @param metadata  The metadata that is used to fetch the start and end times for backing indices of this data stream.
     * @return a backing index with a start time that is greater or equal to the provided timestamp and
     *         an end time that is less than the provided timestamp. Otherwise <code>null</code> is returned.
     */
    public Index selectTimeSeriesWriteIndex(Instant timestamp, Metadata metadata) {
        for (int i = indices.size() - 1; i >= 0; i--) {
            Index index = indices.get(i);
            IndexMetadata im = metadata.index(index);

            // TODO: make index_mode, start and end time fields in IndexMetadata class.
            // (this to avoid the overhead that occurs when reading a setting)
            if (im.getIndexMode() != IndexMode.TIME_SERIES) {
                // Not a tsdb backing index, so skip.
                // (This can happen if this is a migrated tsdb data stream)
                continue;
            }

            Instant start = im.getTimeSeriesStart();
            Instant end = im.getTimeSeriesEnd();
            // Check should be in sync with DataStreamTimestampFieldMapper#validateTimestamp(...) method
            if (timestamp.compareTo(start) >= 0 && timestamp.compareTo(end) < 0) {
                return index;
            }
        }
        return null;
    }

    /**
     * Validates this data stream. If this is a time series data stream then this method validates that temporal range
     * of backing indices (defined by index.time_series.start_time and index.time_series.end_time) do not overlap with each other.
     *
     * @param imSupplier Function that supplies {@link IndexMetadata} instances based on the provided index name
     */
    public void validate(Function<String, IndexMetadata> imSupplier) {
        if (indexMode == IndexMode.TIME_SERIES) {
            // Get a sorted overview of each backing index with there start and end time range:
            var startAndEndTimes = indices.stream().map(index -> {
                IndexMetadata im = imSupplier.apply(index.getName());
                if (im == null) {
                    throw new IllegalStateException("index [" + index.getName() + "] is not found in the index metadata supplier");
                }
                return im;
            })
                .filter(
                    // Migrated tsdb data streams have non tsdb backing indices:
                    im -> im.getTimeSeriesStart() != null && im.getTimeSeriesEnd() != null
                )
                .map(im -> {
                    Instant start = im.getTimeSeriesStart();
                    Instant end = im.getTimeSeriesEnd();
                    assert end.isAfter(start); // This is also validated by TIME_SERIES_END_TIME setting.
                    return new Tuple<>(im.getIndex().getName(), new Tuple<>(start, end));
                })
                .sorted(Comparator.comparing(entry -> entry.v2().v1())) // Sort by start time
                .toList();

            Tuple<String, Tuple<Instant, Instant>> previous = null;
            var formatter = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
            for (var current : startAndEndTimes) {
                if (previous == null) {
                    previous = current;
                } else {
                    // The end_time of previous backing index should be equal or less than start_time of current backing index.
                    // If previous.end_time > current.start_time then we should fail here:
                    if (previous.v2().v2().compareTo(current.v2().v1()) > 0) {
                        String range1 = formatter.format(previous.v2().v1()) + " TO " + formatter.format(previous.v2().v2());
                        String range2 = formatter.format(current.v2().v1()) + " TO " + formatter.format(current.v2().v2());
                        throw new IllegalArgumentException(
                            "backing index ["
                                + previous.v1()
                                + "] with range ["
                                + range1
                                + "] is overlapping with backing index ["
                                + current.v1()
                                + "] with range ["
                                + range2
                                + "]"
                        );
                    }
                }
            }
        }
    }

    @Nullable
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public boolean isHidden() {
        return hidden;
    }

    /**
     * Determines whether this data stream is replicated from elsewhere,
     * for example a remote cluster
     *
     * @return Whether this data stream is replicated.
     */
    public boolean isReplicated() {
        return replicated;
    }

    @Override
    public boolean isSystem() {
        return system;
    }

    public boolean isAllowCustomRouting() {
        return allowCustomRouting;
    }

    /**
     * Determines if this data stream should persist ingest pipeline and mapping failures from bulk requests to a locally
     * configured failure store.
     *
     * @return Whether this data stream should store ingestion failures.
     */
    public boolean isFailureStoreEnabled() {
        return failureStoreEnabled;
    }

    @Nullable
    public IndexMode getIndexMode() {
        return indexMode;
    }

    @Nullable
    public DataStreamLifecycle getLifecycle() {
        return lifecycle;
    }

    /**
     * Returns the latest auto sharding event that happened for this data stream
     */
    public DataStreamAutoShardingEvent getAutoShardingEvent() {
        return autoShardingEvent;
    }

    /**
     * Performs a rollover on a {@code DataStream} instance and returns a new instance containing
     * the updated list of backing indices and incremented generation.
     *
     * @param writeIndex    new write index
     * @param generation    new generation
     * @param timeSeries    whether the template that created this data stream is in time series mode
     * @param autoShardingEvent the auto sharding event this rollover operation is applying
     *
     * @return new {@code DataStream} instance with the rollover operation applied
     */
    public DataStream rollover(
        Index writeIndex,
        long generation,
        boolean timeSeries,
        @Nullable DataStreamAutoShardingEvent autoShardingEvent
    ) {
        ensureNotReplicated();

        return unsafeRollover(writeIndex, generation, timeSeries, autoShardingEvent);
    }

    /**
     * Like {@link #rollover(Index, long, boolean, DataStreamAutoShardingEvent)}, but does no validation, use with care only.
     */
    public DataStream unsafeRollover(Index writeIndex, long generation, boolean timeSeries, DataStreamAutoShardingEvent autoShardingEvent) {
        IndexMode indexMode = this.indexMode;
        if ((indexMode == null || indexMode == IndexMode.STANDARD) && timeSeries) {
            // This allows for migrating a data stream to be a tsdb data stream:
            // (only if index_mode=null|standard then allow it to be set to time_series)
            indexMode = IndexMode.TIME_SERIES;
        } else if (indexMode == IndexMode.TIME_SERIES && timeSeries == false) {
            // Allow downgrading a time series data stream to a regular data stream
            indexMode = null;
        }

        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.add(writeIndex);
        return copy().setIndices(backingIndices)
            .setGeneration(generation)
            .setReplicated(false)
            .setIndexMode(indexMode)
            .setAutoShardingEvent(autoShardingEvent)
            .setRolloverOnWrite(false)
            .build();
    }

    /**
     * Performs a rollover on the failure store of a {@code DataStream} instance and returns a new instance containing
     * the updated list of failure store indices and incremented generation.
     *
     * @param writeIndex new failure store write index
     * @param generation new generation
     * @return new {@code DataStream} instance with the rollover operation applied
     */
    public DataStream rolloverFailureStore(Index writeIndex, long generation) {
        ensureNotReplicated();

        return unsafeRolloverFailureStore(writeIndex, generation);
    }

    /**
     * Like {@link #rolloverFailureStore(Index, long)}, but does no validation, use with care only.
     */
    public DataStream unsafeRolloverFailureStore(Index writeIndex, long generation) {
        List<Index> failureIndices = new ArrayList<>(this.failureIndices);
        failureIndices.add(writeIndex);
        return copy().setGeneration(generation).setReplicated(false).setFailureIndices(failureIndices).build();
    }

    /**
     * Generates the next write index name and <code>generation</code> to be used for rolling over this data stream.
     *
     * @param clusterMetadata Cluster metadata
     * @return tuple of the next write index name and next generation.
     */
    public Tuple<String, Long> nextWriteIndexAndGeneration(Metadata clusterMetadata) {
        ensureNotReplicated();
        return unsafeNextWriteIndexAndGeneration(clusterMetadata);
    }

    /**
     * Like {@link #nextWriteIndexAndGeneration(Metadata)}, but does no validation, use with care only.
     */
    public Tuple<String, Long> unsafeNextWriteIndexAndGeneration(Metadata clusterMetadata) {
        return generateNextWriteIndexAndGeneration(clusterMetadata, DataStream::getDefaultBackingIndexName);
    }

    /**
     * Generates the next write index name and <code>generation</code> to be used for rolling over the failure store of this data stream.
     *
     * @param clusterMetadata Cluster metadata
     * @return tuple of the next failure store write index name and next generation.
     */
    public Tuple<String, Long> nextFailureStoreWriteIndexAndGeneration(Metadata clusterMetadata) {
        ensureNotReplicated();
        return unsafeNextFailureStoreWriteIndexAndGeneration(clusterMetadata);
    }

    /**
     * Like {@link #nextFailureStoreWriteIndexAndGeneration(Metadata)}, but does no validation, use with care only.
     */
    public Tuple<String, Long> unsafeNextFailureStoreWriteIndexAndGeneration(Metadata clusterMetadata) {
        return generateNextWriteIndexAndGeneration(clusterMetadata, DataStream::getDefaultFailureStoreName);
    }

    private Tuple<String, Long> generateNextWriteIndexAndGeneration(
        Metadata clusterMetadata,
        TriFunction<String, Long, Long, String> nameGenerator
    ) {
        String newWriteIndexName;
        long generation = this.generation;
        long currentTimeMillis = timeProvider.getAsLong();
        do {
            newWriteIndexName = nameGenerator.apply(getName(), ++generation, currentTimeMillis);
        } while (clusterMetadata.hasIndexAbstraction(newWriteIndexName));
        return Tuple.tuple(newWriteIndexName, generation);
    }

    private void ensureNotReplicated() {
        if (replicated) {
            throw new IllegalArgumentException("data stream [" + name + "] cannot be rolled over, because it is a replicated data stream");
        }
    }

    /**
     * Removes the specified backing index and returns a new {@code DataStream} instance with
     * the remaining backing indices.
     *
     * @param index the backing index to remove
     * @return new {@code DataStream} instance with the remaining backing indices
     * @throws IllegalArgumentException if {@code index} is not a backing index or is the current write index of the data stream
     */
    public DataStream removeBackingIndex(Index index) {
        int backingIndexPosition = indices.indexOf(index);

        if (backingIndexPosition == -1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "index [%s] is not part of data stream [%s]", index.getName(), name)
            );
        }
        if (indices.size() == (backingIndexPosition + 1)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot remove backing index [%s] of data stream [%s] because it is the write index",
                    index.getName(),
                    name
                )
            );
        }

        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.remove(index);
        assert backingIndices.size() == indices.size() - 1;
        return copy().setIndices(backingIndices).setGeneration(generation + 1).build();
    }

    /**
     * Removes the specified failure store index and returns a new {@code DataStream} instance with
     * the remaining failure store indices.
     *
     * @param index the failure store index to remove
     * @return new {@code DataStream} instance with the remaining failure store indices
     * @throws IllegalArgumentException if {@code index} is not a failure store index or is the current failure store write index of the
     * data stream
     */
    public DataStream removeFailureStoreIndex(Index index) {
        int failureIndexPosition = failureIndices.indexOf(index);

        if (failureIndexPosition == -1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "index [%s] is not part of data stream [%s] failure store", index.getName(), name)
            );
        }

        // TODO: When failure stores are lazily created, this wont necessarily be required anymore. We can remove the failure store write
        // index as long as we mark the data stream to lazily rollover the failure store with no conditions on its next write
        if (failureIndices.size() == (failureIndexPosition + 1)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot remove backing index [%s] of data stream [%s] because it is the write index",
                    index.getName(),
                    name
                )
            );
        }

        List<Index> updatedFailureIndices = new ArrayList<>(failureIndices);
        updatedFailureIndices.remove(index);
        assert updatedFailureIndices.size() == failureIndices.size() - 1;
        return copy().setGeneration(generation + 1).setFailureIndices(updatedFailureIndices).build();
    }

    /**
     * Replaces the specified backing index with a new index and returns a new {@code DataStream} instance with
     * the modified backing indices. An {@code IllegalArgumentException} is thrown if the index to be replaced
     * is not a backing index for this data stream or if it is the {@code DataStream}'s write index.
     *
     * @param existingBackingIndex the backing index to be replaced
     * @param newBackingIndex      the new index that will be part of the {@code DataStream}
     * @return new {@code DataStream} instance with backing indices that contain replacement index instead of the specified
     * existing index.
     */
    public DataStream replaceBackingIndex(Index existingBackingIndex, Index newBackingIndex) {
        List<Index> backingIndices = new ArrayList<>(indices);
        int backingIndexPosition = backingIndices.indexOf(existingBackingIndex);
        if (backingIndexPosition == -1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "index [%s] is not part of data stream [%s]", existingBackingIndex.getName(), name)
            );
        }
        if (indices.size() == (backingIndexPosition + 1)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot replace backing index [%s] of data stream [%s] because it is the write index",
                    existingBackingIndex.getName(),
                    name
                )
            );
        }
        backingIndices.set(backingIndexPosition, newBackingIndex);
        return copy().setIndices(backingIndices).setGeneration(generation + 1).build();
    }

    /**
     * Adds the specified index as a backing index and returns a new {@code DataStream} instance with the new combination
     * of backing indices.
     *
     * @param index index to add to the data stream
     * @return new {@code DataStream} instance with the added backing index
     * @throws IllegalArgumentException if {@code index} is ineligible to be a backing index for the data stream
     */
    public DataStream addBackingIndex(Metadata clusterMetadata, Index index) {
        // validate that index is not part of another data stream
        final var parentDataStream = clusterMetadata.getIndicesLookup().get(index.getName()).getParentDataStream();
        if (parentDataStream != null) {
            validateDataStreamAlreadyContainsIndex(index, parentDataStream, false);
            return this;
        }

        // ensure that no aliases reference index
        ensureNoAliasesOnIndex(clusterMetadata, index);

        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.add(0, index);
        assert backingIndices.size() == indices.size() + 1;
        return copy().setIndices(backingIndices).setGeneration(generation + 1).build();
    }

    /**
     * Adds the specified index as a failure store index and returns a new {@code DataStream} instance with the new combination
     * of failure store indices.
     *
     * @param index index to add to the data stream's failure store
     * @return new {@code DataStream} instance with the added failure store index
     * @throws IllegalArgumentException if {@code index} is ineligible to be a failure store index for the data stream
     */
    public DataStream addFailureStoreIndex(Metadata clusterMetadata, Index index) {
        // validate that index is not part of another data stream
        final var parentDataStream = clusterMetadata.getIndicesLookup().get(index.getName()).getParentDataStream();
        if (parentDataStream != null) {
            validateDataStreamAlreadyContainsIndex(index, parentDataStream, true);
            return this;
        }

        ensureNoAliasesOnIndex(clusterMetadata, index);

        List<Index> updatedFailureIndices = new ArrayList<>(failureIndices);
        updatedFailureIndices.add(0, index);
        assert updatedFailureIndices.size() == failureIndices.size() + 1;
        return copy().setGeneration(generation + 1).setFailureIndices(updatedFailureIndices).build();
    }

    /**
     * Given an index and its parent data stream, determine if the parent data stream is the same as this one, and if it is, check if the
     * index is already in the correct indices list.
     *
     * @param index The index to check for
     * @param parentDataStream The data stream the index already belongs to
     * @param targetFailureStore true if the index should be added to the failure store, false if it should be added to the backing indices
     * @throws IllegalArgumentException if the index belongs to a different data stream, or if it is in the wrong index set
     */
    private void validateDataStreamAlreadyContainsIndex(Index index, DataStream parentDataStream, boolean targetFailureStore) {
        if (parentDataStream.equals(this) == false || (parentDataStream.isFailureStoreIndex(index.getName()) != targetFailureStore)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] because it is already a %s index on data stream [%s]",
                    index.getName(),
                    getName(),
                    parentDataStream.isFailureStoreIndex(index.getName()) ? "failure store" : "backing",
                    parentDataStream.getName()
                )
            );
        }
    }

    private void ensureNoAliasesOnIndex(Metadata clusterMetadata, Index index) {
        IndexMetadata im = clusterMetadata.index(clusterMetadata.getIndicesLookup().get(index.getName()).getWriteIndex());
        if (im.getAliases().size() > 0) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] until its %s [%s] %s removed",
                    index.getName(),
                    getName(),
                    im.getAliases().size() > 1 ? "aliases" : "alias",
                    Strings.collectionToCommaDelimitedString(im.getAliases().keySet().stream().sorted().toList()),
                    im.getAliases().size() > 1 ? "are" : "is"
                )
            );
        }
    }

    public DataStream promoteDataStream() {
        return copy().setReplicated(false).build();
    }

    /**
     * Reconciles this data stream with a list of indices available in a snapshot. Allows snapshots to store accurate data
     * stream definitions that do not reference backing indices not contained in the snapshot.
     *
     * @param indicesInSnapshot List of indices in the snapshot
     * @return Reconciled {@link DataStream} instance or {@code null} if no reconciled version of this data stream could be built from the
     *         given indices
     */
    @Nullable
    public DataStream snapshot(Collection<String> indicesInSnapshot) {
        // do not include indices not available in the snapshot
        List<Index> reconciledIndices = new ArrayList<>(this.indices);
        if (reconciledIndices.removeIf(x -> indicesInSnapshot.contains(x.getName()) == false) == false) {
            return this;
        }

        if (reconciledIndices.size() == 0) {
            return null;
        }

        return copy().setIndices(reconciledIndices).setMetadata(metadata == null ? null : new HashMap<>(metadata)).build();
    }

    /**
     * Iterate over the backing indices and return the ones that are managed by the data stream lifecycle and past the configured
     * retention in their lifecycle.
     * NOTE that this specifically does not return the write index of the data stream as usually retention
     * is treated differently for the write index (i.e. they first need to be rolled over)
     */
    public List<Index> getIndicesPastRetention(
        Function<String, IndexMetadata> indexMetadataSupplier,
        LongSupplier nowSupplier,
        DataStreamGlobalRetention globalRetention
    ) {
        if (lifecycle == null
            || lifecycle.isEnabled() == false
            || lifecycle.getEffectiveDataRetention(isSystem() ? null : globalRetention) == null) {
            return List.of();
        }

        List<Index> indicesPastRetention = getNonWriteIndicesOlderThan(
            lifecycle.getEffectiveDataRetention(isSystem() ? null : globalRetention),
            indexMetadataSupplier,
            this::isIndexManagedByDataStreamLifecycle,
            nowSupplier
        );
        return indicesPastRetention;
    }

    /**
     * Returns a list of downsampling rounds this index is eligible for (based on the rounds `after` configuration) or
     * an empty list if this data streams' lifecycle doesn't have downsampling configured or the index's generation age
     * doesn't yet match any `after` downsampling configuration.
     *
     * An empty list is returned for indices that are not time series.
     */
    public List<Round> getDownsamplingRoundsFor(
        Index index,
        Function<String, IndexMetadata> indexMetadataSupplier,
        LongSupplier nowSupplier
    ) {
        assert indices.contains(index) : "the provided index must be a backing index for this datastream";
        if (lifecycle == null || lifecycle.getDownsamplingRounds() == null) {
            return List.of();
        }

        IndexMetadata indexMetadata = indexMetadataSupplier.apply(index.getName());
        if (indexMetadata == null || IndexSettings.MODE.get(indexMetadata.getSettings()) != IndexMode.TIME_SERIES) {
            return List.of();
        }
        TimeValue indexGenerationTime = getGenerationLifecycleDate(indexMetadata);

        if (indexGenerationTime != null) {
            long nowMillis = nowSupplier.getAsLong();
            long indexGenerationTimeMillis = indexGenerationTime.millis();
            List<Round> orderedRoundsForIndex = new ArrayList<>(lifecycle.getDownsamplingRounds().size());
            for (Round round : lifecycle.getDownsamplingRounds()) {
                if (nowMillis >= indexGenerationTimeMillis + round.after().getMillis()) {
                    orderedRoundsForIndex.add(round);
                }
            }
            return orderedRoundsForIndex;
        }
        return List.of();
    }

    /**
     * Returns the non-write backing indices that are older than the provided age, *excluding the write index*.
     * The index age is calculated from the rollover or index creation date (or the origination date if present).
     * If an indices predicate is provided the returned list of indices will be filtered
     * according to the predicate definition. This is useful for things like "return only
     * the backing indices that are managed by the data stream lifecycle".
     */
    public List<Index> getNonWriteIndicesOlderThan(
        TimeValue retentionPeriod,
        Function<String, IndexMetadata> indexMetadataSupplier,
        @Nullable Predicate<IndexMetadata> indicesPredicate,
        LongSupplier nowSupplier
    ) {
        List<Index> olderIndices = new ArrayList<>();
        for (Index index : indices) {
            if (isIndexOderThan(index, retentionPeriod.getMillis(), nowSupplier.getAsLong(), indicesPredicate, indexMetadataSupplier)) {
                olderIndices.add(index);
            }
        }
        return olderIndices;
    }

    private boolean isIndexOderThan(
        Index index,
        long retentionPeriod,
        long now,
        Predicate<IndexMetadata> indicesPredicate,
        Function<String, IndexMetadata> indexMetadataSupplier
    ) {
        IndexMetadata indexMetadata = indexMetadataSupplier.apply(index.getName());
        if (indexMetadata == null) {
            // we would normally throw exception in a situation like this however, this is meant to be a helper method
            // so let's ignore deleted indices
            return false;
        }
        TimeValue indexLifecycleDate = getGenerationLifecycleDate(indexMetadata);
        return indexLifecycleDate != null
            && now >= indexLifecycleDate.getMillis() + retentionPeriod
            && (indicesPredicate == null || indicesPredicate.test(indexMetadata));
    }

    /**
     * Checks if the provided backing index is managed by the data stream lifecycle as part of this data stream.
     * If the index is not a backing index of this data stream, or we cannot supply its metadata
     * we return false.
     */
    public boolean isIndexManagedByDataStreamLifecycle(Index index, Function<String, IndexMetadata> indexMetadataSupplier) {
        if (indices.contains(index) == false) {
            return false;
        }
        IndexMetadata indexMetadata = indexMetadataSupplier.apply(index.getName());
        if (indexMetadata == null) {
            // the index was deleted
            return false;
        }
        return isIndexManagedByDataStreamLifecycle(indexMetadata);
    }

    /**
     * This is the raw definition of an index being managed by the data stream lifecycle. An index is managed by the data stream lifecycle
     * if it's part of a data stream that has a data stream lifecycle configured and enabled and depending on the value of
     * {@link org.elasticsearch.index.IndexSettings#PREFER_ILM_SETTING} having an ILM policy configured will play into the decision.
     * This method also skips any validation to make sure the index is part of this data stream, hence the private
     * access method.
     */
    private boolean isIndexManagedByDataStreamLifecycle(IndexMetadata indexMetadata) {
        if (indexMetadata.getLifecyclePolicyName() != null && lifecycle != null && lifecycle.isEnabled()) {
            // when both ILM and data stream lifecycle are configured, choose depending on the configured preference for this backing index
            return PREFER_ILM_SETTING.get(indexMetadata.getSettings()) == false;
        }
        return lifecycle != null && lifecycle.isEnabled();
    }

    /**
     * Returns the generation date of the index whose metadata is passed. The generation date of the index represents the time at which the
     * index started progressing towards the user configurable / business specific parts of the lifecycle (e.g. retention).
     * The generation date is the origination date if it exists, or the rollover date if it exists and the origination date does not, or
     * the creation date if neither the origination date nor the rollover date exist.
     * If the index is the write index the generation date will be null because it is not eligible for retention or other parts of the
     * lifecycle.
     * @param indexMetadata The metadata of the index whose generation date is returned
     * @return The generation date of the index, or null if this is the write index
     */
    @Nullable
    public TimeValue getGenerationLifecycleDate(IndexMetadata indexMetadata) {
        if (indexMetadata.getIndex().equals(getWriteIndex())) {
            return null;
        }
        Long originationDate = indexMetadata.getSettings().getAsLong(LIFECYCLE_ORIGINATION_DATE, null);
        RolloverInfo rolloverInfo = indexMetadata.getRolloverInfos().get(getName());
        if (rolloverInfo != null) {
            return TimeValue.timeValueMillis(Objects.requireNonNullElseGet(originationDate, rolloverInfo::getTime));
        } else {
            return TimeValue.timeValueMillis(Objects.requireNonNullElseGet(originationDate, indexMetadata::getCreationDate));
        }
    }

    /**
     * Generates the name of the index that conforms to the default naming convention for backing indices
     * on data streams given the specified data stream name and generation and the current system time.
     *
     * @param dataStreamName name of the data stream
     * @param generation generation of the data stream
     * @return backing index name
     */
    public static String getDefaultBackingIndexName(String dataStreamName, long generation) {
        return getDefaultBackingIndexName(dataStreamName, generation, System.currentTimeMillis());
    }

    /**
     * Generates the name of the index that conforms to the default naming convention for backing indices
     * on data streams given the specified data stream name, generation, and time.
     *
     * @param dataStreamName name of the data stream
     * @param generation generation of the data stream
     * @param epochMillis creation time for the backing index
     * @return backing index name
     */
    public static String getDefaultBackingIndexName(String dataStreamName, long generation, long epochMillis) {
        return String.format(
            Locale.ROOT,
            BACKING_INDEX_PREFIX + "%s-%s-%06d",
            dataStreamName,
            DATE_FORMATTER.formatMillis(epochMillis),
            generation
        );
    }

    /**
     * Generates the name of the index that conforms to the default naming convention for backing indices
     * on data streams given the specified data stream name, generation, and time.
     *
     * @param dataStreamName name of the data stream
     * @param generation generation of the data stream
     * @param epochMillis creation time for the backing index
     * @return backing index name
     */
    public static String getDefaultFailureStoreName(String dataStreamName, long generation, long epochMillis) {
        return String.format(
            Locale.ROOT,
            FAILURE_STORE_PREFIX + "%s-%s-%06d",
            dataStreamName,
            DATE_FORMATTER.formatMillis(epochMillis),
            generation
        );
    }

    public DataStream(StreamInput in) throws IOException {
        this(
            readName(in),
            readIndices(in),
            in.readVLong(),
            in.readGenericMap(),
            in.readBoolean(),
            in.readBoolean(),
            in.readBoolean(),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0) ? in.readBoolean() : false,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0) ? in.readOptionalEnum(IndexMode.class) : null,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X) ? in.readOptionalWriteable(DataStreamLifecycle::new) : null,
            in.getTransportVersion().onOrAfter(DataStream.ADDED_FAILURE_STORE_TRANSPORT_VERSION) ? in.readBoolean() : false,
            in.getTransportVersion().onOrAfter(DataStream.ADDED_FAILURE_STORE_TRANSPORT_VERSION) ? readIndices(in) : List.of(),
            in.getTransportVersion().onOrAfter(TransportVersions.LAZY_ROLLOVER_ADDED) ? in.readBoolean() : false,
            in.getTransportVersion().onOrAfter(DataStream.ADDED_AUTO_SHARDING_EVENT_VERSION)
                ? in.readOptionalWriteable(DataStreamAutoShardingEvent::new)
                : null
        );
    }

    static String readName(StreamInput in) throws IOException {
        String name = in.readString();
        in.readString(); // TODO: clear out the timestamp field, which is a constant https://github.com/elastic/elasticsearch/issues/101991
        return name;
    }

    static List<Index> readIndices(StreamInput in) throws IOException {
        return in.readCollectionAsImmutableList(Index::new);
    }

    public static Diff<DataStream> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStream::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(TIMESTAMP_FIELD_NAME); // TODO: clear this out in the future https://github.com/elastic/elasticsearch/issues/101991
        out.writeCollection(indices);
        out.writeVLong(generation);
        out.writeGenericMap(metadata);
        out.writeBoolean(hidden);
        out.writeBoolean(replicated);
        out.writeBoolean(system);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            out.writeBoolean(allowCustomRouting);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0)) {
            out.writeOptionalEnum(indexMode);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeOptionalWriteable(lifecycle);
        }
        if (out.getTransportVersion().onOrAfter(DataStream.ADDED_FAILURE_STORE_TRANSPORT_VERSION)) {
            out.writeBoolean(failureStoreEnabled);
            out.writeCollection(failureIndices);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.LAZY_ROLLOVER_ADDED)) {
            out.writeBoolean(rolloverOnWrite);
        }
        if (out.getTransportVersion().onOrAfter(DataStream.ADDED_AUTO_SHARDING_EVENT_VERSION)) {
            out.writeOptionalWriteable(autoShardingEvent);
        }
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField TIMESTAMP_FIELD_FIELD = new ParseField("timestamp_field");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField GENERATION_FIELD = new ParseField("generation");
    public static final ParseField METADATA_FIELD = new ParseField("_meta");
    public static final ParseField HIDDEN_FIELD = new ParseField("hidden");
    public static final ParseField REPLICATED_FIELD = new ParseField("replicated");
    public static final ParseField SYSTEM_FIELD = new ParseField("system");
    public static final ParseField ALLOW_CUSTOM_ROUTING = new ParseField("allow_custom_routing");
    public static final ParseField INDEX_MODE = new ParseField("index_mode");
    public static final ParseField LIFECYCLE = new ParseField("lifecycle");
    public static final ParseField FAILURE_STORE_FIELD = new ParseField("failure_store");
    public static final ParseField FAILURE_INDICES_FIELD = new ParseField("failure_indices");
    public static final ParseField ROLLOVER_ON_WRITE_FIELD = new ParseField("rollover_on_write");
    public static final ParseField AUTO_SHARDING_FIELD = new ParseField("auto_sharding");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>("data_stream", args -> {
        // Fields behind a feature flag need to be parsed last otherwise the parser will fail when the feature flag is disabled.
        // Until the feature flag is removed we keep them separately to be mindful of this.
        boolean failureStoreEnabled = DataStream.isFailureStoreFeatureFlagEnabled() && args[12] != null && (boolean) args[12];
        List<Index> failureStoreIndices = DataStream.isFailureStoreFeatureFlagEnabled() && args[13] != null
            ? (List<Index>) args[13]
            : List.of();
        return new DataStream(
            (String) args[0],
            (List<Index>) args[1],
            (Long) args[2],
            (Map<String, Object>) args[3],
            args[4] != null && (boolean) args[4],
            args[5] != null && (boolean) args[5],
            args[6] != null && (boolean) args[6],
            args[7] != null && (boolean) args[7],
            args[8] != null ? IndexMode.fromString((String) args[8]) : null,
            (DataStreamLifecycle) args[9],
            failureStoreEnabled,
            failureStoreIndices,
            args[10] != null && (boolean) args[10],
            (DataStreamAutoShardingEvent) args[11]
        );
    });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        final ConstructingObjectParser<String, Void> tsFieldParser = new ConstructingObjectParser<>("timestamp_field", args -> {
            if (TIMESTAMP_FIELD_NAME.equals(args[0]) == false) {
                throw new IllegalArgumentException("unexpected timestamp field [" + args[0] + "]");
            }
            return TIMESTAMP_FIELD_NAME;
        });
        tsFieldParser.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject((f, v) -> { assert v == TIMESTAMP_FIELD_NAME; }, tsFieldParser, TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Index.fromXContent(p), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), HIDDEN_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), REPLICATED_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), SYSTEM_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ALLOW_CUSTOM_ROUTING);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_MODE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> DataStreamLifecycle.fromXContent(p), LIFECYCLE);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ROLLOVER_ON_WRITE_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamAutoShardingEvent.fromXContent(p),
            AUTO_SHARDING_FIELD
        );
        // The fields behind the feature flag should always be last.
        if (DataStream.isFailureStoreFeatureFlagEnabled()) {
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FAILURE_STORE_FIELD);
            PARSER.declareObjectArray(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> Index.fromXContent(p),
                FAILURE_INDICES_FIELD
            );
        }
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null, null);
    }

    /**
     * Converts the data stream to XContent and passes the RolloverConditions, when provided, to the lifecycle.
     */
    public XContentBuilder toXContent(
        XContentBuilder builder,
        Params params,
        @Nullable RolloverConfiguration rolloverConfiguration,
        @Nullable DataStreamGlobalRetention globalRetention
    ) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(TIMESTAMP_FIELD_FIELD.getPreferredName())
            .startObject()
            .field(NAME_FIELD.getPreferredName(), TIMESTAMP_FIELD_NAME)
            .endObject();
        builder.xContentList(INDICES_FIELD.getPreferredName(), indices);
        builder.field(GENERATION_FIELD.getPreferredName(), generation);
        if (DataStream.isFailureStoreFeatureFlagEnabled() && failureIndices.isEmpty() == false) {
            builder.xContentList(FAILURE_INDICES_FIELD.getPreferredName(), failureIndices);
        }
        if (metadata != null) {
            builder.field(METADATA_FIELD.getPreferredName(), metadata);
        }
        builder.field(HIDDEN_FIELD.getPreferredName(), hidden);
        builder.field(REPLICATED_FIELD.getPreferredName(), replicated);
        builder.field(SYSTEM_FIELD.getPreferredName(), system);
        builder.field(ALLOW_CUSTOM_ROUTING.getPreferredName(), allowCustomRouting);
        if (DataStream.isFailureStoreFeatureFlagEnabled()) {
            builder.field(FAILURE_STORE_FIELD.getPreferredName(), failureStoreEnabled);
        }
        if (indexMode != null) {
            builder.field(INDEX_MODE.getPreferredName(), indexMode);
        }
        if (lifecycle != null) {
            builder.field(LIFECYCLE.getPreferredName());
            lifecycle.toXContent(builder, params, rolloverConfiguration, isSystem() ? null : globalRetention);
        }
        builder.field(ROLLOVER_ON_WRITE_FIELD.getPreferredName(), rolloverOnWrite);
        if (autoShardingEvent != null) {
            builder.startObject(AUTO_SHARDING_FIELD.getPreferredName());
            autoShardingEvent.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStream that = (DataStream) o;
        return name.equals(that.name)
            && indices.equals(that.indices)
            && generation == that.generation
            && Objects.equals(metadata, that.metadata)
            && hidden == that.hidden
            && system == that.system
            && replicated == that.replicated
            && allowCustomRouting == that.allowCustomRouting
            && indexMode == that.indexMode
            && Objects.equals(lifecycle, that.lifecycle)
            && failureStoreEnabled == that.failureStoreEnabled
            && failureIndices.equals(that.failureIndices)
            && rolloverOnWrite == that.rolloverOnWrite
            && Objects.equals(autoShardingEvent, that.autoShardingEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            indices,
            generation,
            metadata,
            hidden,
            system,
            replicated,
            allowCustomRouting,
            indexMode,
            lifecycle,
            failureStoreEnabled,
            failureIndices,
            rolloverOnWrite,
            autoShardingEvent
        );
    }

    @Override
    public Index getWriteIndex(IndexRequest request, Metadata metadata) {
        if (request.opType() != DocWriteRequest.OpType.CREATE) {
            return getWriteIndex();
        }

        if (getIndexMode() != IndexMode.TIME_SERIES) {
            return getWriteIndex();
        }

        Instant timestamp;
        Object rawTimestamp = request.getRawTimestamp();
        if (rawTimestamp != null) {
            timestamp = getTimeStampFromRaw(rawTimestamp);
        } else {
            timestamp = getTimestampFromParser(request.source(), request.getContentType());
        }
        timestamp = getCanonicalTimestampBound(timestamp);
        Index result = selectTimeSeriesWriteIndex(timestamp, metadata);
        if (result == null) {
            String timestampAsString = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(timestamp);
            String writeableIndicesString = getIndices().stream()
                .map(metadata::index)
                .map(IndexMetadata::getSettings)
                .map(
                    settings -> "["
                        + settings.get(IndexSettings.TIME_SERIES_START_TIME.getKey())
                        + ","
                        + settings.get(IndexSettings.TIME_SERIES_END_TIME.getKey())
                        + "]"
                )
                .collect(Collectors.joining());
            throw new IllegalArgumentException(
                "the document timestamp ["
                    + timestampAsString
                    + "] is outside of ranges of currently writable indices ["
                    + writeableIndicesString
                    + "]"
            );
        }
        return result;
    }

    @Override
    public DataStream getParentDataStream() {
        // a data stream cannot have a parent data stream
        return null;
    }

    public static final XContentParserConfiguration TS_EXTRACT_CONFIG = XContentParserConfiguration.EMPTY.withFiltering(
        Set.of(TIMESTAMP_FIELD_NAME),
        null,
        false
    );

    private static final DateFormatter TIMESTAMP_FORMATTER = DateFormatter.forPattern(
        "strict_date_optional_time_nanos||strict_date_optional_time||epoch_millis"
    );

    /**
     * Returns the indices created within the {@param maxIndexAge} interval. Note that this strives to cover
     * the entire {@param maxIndexAge} interval so one backing index created before the specified age will also
     * be return.
     */
    public static List<Index> getIndicesWithinMaxAgeRange(
        DataStream dataStream,
        Function<Index, IndexMetadata> indexProvider,
        TimeValue maxIndexAge,
        LongSupplier nowSupplier
    ) {
        final List<Index> dataStreamIndices = dataStream.getIndices();
        final long currentTimeMillis = nowSupplier.getAsLong();
        // Consider at least 1 index (including the write index) for cases where rollovers happen less often than maxIndexAge
        int firstIndexWithinAgeRange = Math.max(dataStreamIndices.size() - 2, 0);
        for (int i = 0; i < dataStreamIndices.size(); i++) {
            Index index = dataStreamIndices.get(i);
            final IndexMetadata indexMetadata = indexProvider.apply(index);
            final long indexAge = currentTimeMillis - indexMetadata.getCreationDate();
            if (indexAge < maxIndexAge.getMillis()) {
                // We need to consider the previous index too in order to cover the entire max-index-age range.
                firstIndexWithinAgeRange = i == 0 ? 0 : i - 1;
                break;
            }
        }
        return dataStreamIndices.subList(firstIndexWithinAgeRange, dataStreamIndices.size());
    }

    private static Instant getTimeStampFromRaw(Object rawTimestamp) {
        try {
            if (rawTimestamp instanceof Long lTimestamp) {
                return Instant.ofEpochMilli(lTimestamp);
            } else if (rawTimestamp instanceof String sTimestamp) {
                return DateFormatters.from(TIMESTAMP_FORMATTER.parse(sTimestamp), TIMESTAMP_FORMATTER.locale()).toInstant();
            } else {
                throw new IllegalArgumentException("timestamp [" + rawTimestamp + "] type [" + rawTimestamp.getClass() + "] error");
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Error get data stream timestamp field: " + e.getMessage(), e);
        }
    }

    private static Instant getTimestampFromParser(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(TS_EXTRACT_CONFIG, source, xContentType)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            return switch (parser.nextToken()) {
                case VALUE_STRING -> DateFormatters.from(TIMESTAMP_FORMATTER.parse(parser.text()), TIMESTAMP_FORMATTER.locale())
                    .toInstant();
                case VALUE_NUMBER -> Instant.ofEpochMilli(parser.longValue());
                default -> throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(
                        Locale.ROOT,
                        "Failed to parse object: expecting token of type [%s] or [%s] but found [%s]",
                        XContentParser.Token.VALUE_STRING,
                        XContentParser.Token.VALUE_NUMBER,
                        parser.currentToken()
                    )
                );
            };
        } catch (Exception e) {
            throw new IllegalArgumentException("Error extracting data stream timestamp field: " + e.getMessage(), e);
        }
    }

    /**
     * Modifies the passed Instant object to be used as a bound for a timestamp field in TimeSeries. It needs to be called in both backing
     * index construction (rollover) and index selection for doc insertion. Failure to do so may lead to errors due to document timestamps
     * exceeding the end time of the selected backing index for insertion.
     * @param time The initial Instant object that's used to generate the canonical time
     * @return A canonical Instant object to be used as a timestamp bound
     */
    public static Instant getCanonicalTimestampBound(Instant time) {
        return time.truncatedTo(ChronoUnit.SECONDS);
    }

    public static Builder builder(String name, List<Index> indices) {
        return new Builder(name, indices);
    }

    public Builder copy() {
        return new Builder(this);
    }

    public static class Builder {
        private LongSupplier timeProvider = System::currentTimeMillis;
        private String name;
        private List<Index> indices;
        private long generation = 1;
        @Nullable
        private Map<String, Object> metadata = null;
        private boolean hidden = false;
        private boolean replicated = false;
        private boolean system = false;
        private boolean allowCustomRouting = false;
        @Nullable
        private IndexMode indexMode = null;
        @Nullable
        private DataStreamLifecycle lifecycle = null;
        private boolean rolloverOnWrite = false;
        private boolean failureStoreEnabled = false;
        private List<Index> failureIndices = List.of();
        @Nullable
        private DataStreamAutoShardingEvent autoShardingEvent = null;

        public Builder(String name, List<Index> indices) {
            this.name = name;
            assert indices.isEmpty() == false : "Cannot create data stream with empty backing indices";
            this.indices = indices;
        }

        public Builder(DataStream dataStream) {
            timeProvider = dataStream.timeProvider;
            name = dataStream.name;
            indices = dataStream.indices;
            generation = dataStream.generation;
            metadata = dataStream.metadata;
            hidden = dataStream.hidden;
            replicated = dataStream.replicated;
            system = dataStream.system;
            allowCustomRouting = dataStream.allowCustomRouting;
            indexMode = dataStream.indexMode;
            lifecycle = dataStream.lifecycle;
            rolloverOnWrite = dataStream.rolloverOnWrite;
            failureStoreEnabled = dataStream.failureStoreEnabled;
            failureIndices = dataStream.failureIndices;
            autoShardingEvent = dataStream.autoShardingEvent;
        }

        public Builder setTimeProvider(LongSupplier timeProvider) {
            this.timeProvider = timeProvider;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setIndices(List<Index> indices) {
            assert indices.isEmpty() == false : "Cannot create data stream with empty backing indices";
            this.indices = indices;
            return this;
        }

        public Builder setGeneration(long generation) {
            this.generation = generation;
            return this;
        }

        public Builder setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setHidden(boolean hidden) {
            this.hidden = hidden;
            return this;
        }

        public Builder setReplicated(boolean replicated) {
            this.replicated = replicated;
            return this;
        }

        public Builder setSystem(boolean system) {
            this.system = system;
            return this;
        }

        public Builder setAllowCustomRouting(boolean allowCustomRouting) {
            this.allowCustomRouting = allowCustomRouting;
            return this;
        }

        public Builder setIndexMode(IndexMode indexMode) {
            this.indexMode = indexMode;
            return this;
        }

        public Builder setLifecycle(DataStreamLifecycle lifecycle) {
            this.lifecycle = lifecycle;
            return this;
        }

        public Builder setRolloverOnWrite(boolean rolloverOnWrite) {
            this.rolloverOnWrite = rolloverOnWrite;
            return this;
        }

        public Builder setFailureStoreEnabled(boolean failureStoreEnabled) {
            this.failureStoreEnabled = failureStoreEnabled;
            return this;
        }

        public Builder setFailureIndices(List<Index> failureIndices) {
            this.failureIndices = failureIndices;
            return this;
        }

        public Builder setAutoShardingEvent(DataStreamAutoShardingEvent autoShardingEvent) {
            this.autoShardingEvent = autoShardingEvent;
            return this;
        }

        public DataStream build() {
            return new DataStream(
                name,
                indices,
                generation,
                metadata,
                hidden,
                replicated,
                system,
                timeProvider,
                allowCustomRouting,
                indexMode,
                lifecycle,
                failureStoreEnabled,
                failureIndices,
                rolloverOnWrite,
                autoShardingEvent
            );
        }
    }
}
