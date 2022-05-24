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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
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

import java.io.IOException;
import java.time.Instant;
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

public final class DataStream implements SimpleDiffable<DataStream>, ToXContentObject {

    public static final String BACKING_INDEX_PREFIX = ".ds-";
    public static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");
    public static final TimestampField TIMESTAMP_FIELD = new DataStream.TimestampField("@timestamp");
    // Timeseries indices' leaf readers should be sorted by desc order of their timestamp field, as it allows search time optimizations
    public static Comparator<LeafReader> TIMESERIES_LEAF_READERS_SORTER = Comparator.comparingLong((LeafReader r) -> {
        try {
            PointValues points = r.getPointValues(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);
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
            throw new ElasticsearchException(
                "Can't access [" + DataStream.TimestampField.FIXED_TIMESTAMP_FIELD + "] field for the index!",
                e
            );
        }
    }).reversed();

    private final LongSupplier timeProvider;
    private final String name;
    private final List<Index> indices;
    private final long generation;
    private final Map<String, Object> metadata;
    private final boolean hidden;
    private final boolean replicated;
    private final boolean system;
    private final boolean allowCustomRouting;
    private final IndexMode indexMode;

    public DataStream(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean hidden,
        boolean replicated,
        boolean system,
        boolean allowCustomRouting,
        IndexMode indexMode
    ) {
        this(name, indices, generation, metadata, hidden, replicated, system, System::currentTimeMillis, allowCustomRouting, indexMode);
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
        IndexMode indexMode
    ) {
        this.name = name;
        this.indices = List.copyOf(indices);
        this.generation = generation;
        this.metadata = metadata;
        assert system == false || hidden; // system indices must be hidden
        this.hidden = hidden;
        this.replicated = replicated;
        this.timeProvider = timeProvider;
        this.system = system;
        this.allowCustomRouting = allowCustomRouting;
        this.indexMode = indexMode;
        assert assertConsistent(this.indices);
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

    public String getName() {
        return name;
    }

    public TimestampField getTimeStampField() {
        // This was always fixed to @timestamp with the idea that one day this field could be configurable. This idea no longer exists.
        return TIMESTAMP_FIELD;
    }

    public List<Index> getIndices() {
        return indices;
    }

    public long getGeneration() {
        return generation;
    }

    public Index getWriteIndex() {
        return indices.get(indices.size() - 1);
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
            if (IndexSettings.MODE.get(im.getSettings()) != IndexMode.TIME_SERIES) {
                // Not a tsdb backing index, so skip.
                // (This can happen is this is a migrated tsdb data stream)
                continue;
            }

            Instant start = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
            Instant end = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
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
            var startAndEndTimes = indices.stream()
                .map(index -> imSupplier.apply(index.getName()))
                .filter(
                    // Migrated tsdb data streams have non tsdb backing indices:
                    im -> IndexSettings.TIME_SERIES_START_TIME.exists(im.getSettings())
                        && IndexSettings.TIME_SERIES_END_TIME.exists(im.getSettings())
                )
                .map(im -> {
                    Instant start = IndexSettings.TIME_SERIES_START_TIME.get(im.getSettings());
                    Instant end = IndexSettings.TIME_SERIES_END_TIME.get(im.getSettings());
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

    public boolean isHidden() {
        return hidden;
    }

    /**
     * Determines whether this data stream is replicated from elsewhere,
     * for example a remote cluster.
     *
     * @return Whether this data stream is replicated.
     */
    public boolean isReplicated() {
        return replicated;
    }

    public boolean isSystem() {
        return system;
    }

    public boolean isAllowCustomRouting() {
        return allowCustomRouting;
    }

    @Nullable
    public IndexMode getIndexMode() {
        return indexMode;
    }

    /**
     * Performs a rollover on a {@code DataStream} instance and returns a new instance containing
     * the updated list of backing indices and incremented generation.
     *
     * @param writeIndex            new write index
     * @param generation            new generation
     * @param indexModeFromTemplate the index mode as is defined in the template that created this data stream
     *
     * @return new {@code DataStream} instance with the rollover operation applied
     */
    public DataStream rollover(Index writeIndex, long generation, IndexMode indexModeFromTemplate) {
        ensureNotReplicated();

        return unsafeRollover(writeIndex, generation, indexModeFromTemplate);
    }

    /**
     * Like {@link #rollover(Index, long, IndexMode)}, but does no validation, use with care only.
     */
    public DataStream unsafeRollover(Index writeIndex, long generation, IndexMode indexModeFromTemplate) {
        IndexMode indexMode = this.indexMode;
        // This allows for migrating a data stream to be a tsdb data stream:
        // (only if index_mode=null|standard then allow it to be set to time_series)
        if ((indexMode == null || indexMode == IndexMode.STANDARD) && indexModeFromTemplate == IndexMode.TIME_SERIES) {
            indexMode = IndexMode.TIME_SERIES;
        }

        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.add(writeIndex);
        return new DataStream(name, backingIndices, generation, metadata, hidden, false, system, allowCustomRouting, indexMode);
    }

    /**
     * Performs a dummy rollover on a {@code DataStream} instance and returns the tuple of the next write index name and next generation
     * that this {@code DataStream} should roll over to using {@link #rollover(Index, long, IndexMode)}.
     *
     * @param clusterMetadata Cluster metadata
     *
     * @return new {@code DataStream} instance with the dummy rollover operation applied
     */
    public Tuple<String, Long> nextWriteIndexAndGeneration(Metadata clusterMetadata) {
        ensureNotReplicated();
        return unsafeNextWriteIndexAndGeneration(clusterMetadata);
    }

    /**
     * Like {@link #nextWriteIndexAndGeneration(Metadata)}, but does no validation, use with care only.
     */
    public Tuple<String, Long> unsafeNextWriteIndexAndGeneration(Metadata clusterMetadata) {
        String newWriteIndexName;
        long generation = this.generation;
        long currentTimeMillis = timeProvider.getAsLong();
        do {
            newWriteIndexName = DataStream.getDefaultBackingIndexName(getName(), ++generation, currentTimeMillis);
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
        return new DataStream(name, backingIndices, generation + 1, metadata, hidden, replicated, system, allowCustomRouting, indexMode);
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
        return new DataStream(name, backingIndices, generation + 1, metadata, hidden, replicated, system, allowCustomRouting, indexMode);
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
            if (parentDataStream.getDataStream().equals(this)) {
                return this;
            } else {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "cannot add index [%s] to data stream [%s] because it is already a backing index on data stream [%s]",
                        index.getName(),
                        getName(),
                        parentDataStream.getName()
                    )
                );
            }
        }

        // ensure that no aliases reference index
        IndexMetadata im = clusterMetadata.index(clusterMetadata.getIndicesLookup().get(index.getName()).getWriteIndex());
        if (im.getAliases().size() > 0) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot add index [%s] to data stream [%s] until its alias(es) [%s] are removed",
                    index.getName(),
                    getName(),
                    Strings.collectionToCommaDelimitedString(im.getAliases().keySet().stream().sorted().toList())
                )
            );
        }

        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.add(0, index);
        assert backingIndices.size() == indices.size() + 1;
        return new DataStream(name, backingIndices, generation + 1, metadata, hidden, replicated, system, allowCustomRouting, indexMode);
    }

    public DataStream promoteDataStream() {
        return new DataStream(name, indices, getGeneration(), metadata, hidden, false, system, timeProvider, allowCustomRouting, indexMode);
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

        return new DataStream(
            name,
            reconciledIndices,
            generation,
            metadata == null ? null : new HashMap<>(metadata),
            hidden,
            replicated,
            system,
            allowCustomRouting,
            indexMode
        );
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

    public DataStream(StreamInput in) throws IOException {
        this(
            in.readString(),
            readIndices(in),
            in.readVLong(),
            in.readMap(),
            in.readBoolean(),
            in.readBoolean(),
            in.readBoolean(),
            in.getVersion().onOrAfter(Version.V_8_0_0) ? in.readBoolean() : false,
            in.getVersion().onOrAfter(Version.V_8_1_0) ? in.readOptionalEnum(IndexMode.class) : null
        );
    }

    static List<Index> readIndices(StreamInput in) throws IOException {
        in.readString(); // timestamp field, which is always @timestamp
        return in.readList(Index::new);
    }

    public static Diff<DataStream> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStream::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        TIMESTAMP_FIELD.writeTo(out);
        out.writeList(indices);
        out.writeVLong(generation);
        out.writeGenericMap(metadata);
        out.writeBoolean(hidden);
        out.writeBoolean(replicated);
        out.writeBoolean(system);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeBoolean(allowCustomRouting);
        }
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeOptionalEnum(indexMode);
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

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>("data_stream", args -> {
        assert TIMESTAMP_FIELD == args[1];
        return new DataStream(
            (String) args[0],
            (List<Index>) args[2],
            (Long) args[3],
            (Map<String, Object>) args[4],
            args[5] != null && (boolean) args[5],
            args[6] != null && (boolean) args[6],
            args[7] != null && (boolean) args[7],
            args[8] != null && (boolean) args[8],
            args[9] != null ? IndexMode.fromString((String) args[9]) : null
        );
    });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), TimestampField.PARSER, TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Index.fromXContent(p), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), HIDDEN_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), REPLICATED_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), SYSTEM_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ALLOW_CUSTOM_ROUTING);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_MODE);
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(TIMESTAMP_FIELD_FIELD.getPreferredName(), TIMESTAMP_FIELD);
        builder.xContentList(INDICES_FIELD.getPreferredName(), indices);
        builder.field(GENERATION_FIELD.getPreferredName(), generation);
        if (metadata != null) {
            builder.field(METADATA_FIELD.getPreferredName(), metadata);
        }
        builder.field(HIDDEN_FIELD.getPreferredName(), hidden);
        builder.field(REPLICATED_FIELD.getPreferredName(), replicated);
        builder.field(SYSTEM_FIELD.getPreferredName(), system);
        builder.field(ALLOW_CUSTOM_ROUTING.getPreferredName(), allowCustomRouting);
        if (indexMode != null) {
            builder.field(INDEX_MODE.getPreferredName(), indexMode);
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
            && replicated == that.replicated
            && allowCustomRouting == that.allowCustomRouting
            && indexMode == that.indexMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, indices, generation, metadata, hidden, replicated, allowCustomRouting, indexMode);
    }

    public static final class TimestampField implements Writeable, ToXContentObject {

        public static final String FIXED_TIMESTAMP_FIELD = "@timestamp";

        static ParseField NAME_FIELD = new ParseField("name");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<TimestampField, Void> PARSER = new ConstructingObjectParser<>(
            "timestamp_field",
            args -> {
                if (FIXED_TIMESTAMP_FIELD.equals(args[0]) == false) {
                    throw new IllegalArgumentException("unexpected timestamp field [" + args[0] + "]");
                }
                return TIMESTAMP_FIELD;
            }
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        }

        private final String name;

        public TimestampField(String name) {
            if (FIXED_TIMESTAMP_FIELD.equals(name) == false) {
                throw new IllegalArgumentException("unexpected timestamp field [" + name + "]");
            }
            this.name = name;
        }

        public TimestampField(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.endObject();
            return builder;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampField that = (TimestampField) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }
}
