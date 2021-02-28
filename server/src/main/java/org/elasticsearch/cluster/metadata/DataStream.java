/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class DataStream extends AbstractDiffable<DataStream> implements ToXContentObject {

    public static final String BACKING_INDEX_PREFIX = ".ds-";
    public static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");

    /**
     * The version when data stream metadata, hidden and replicated data streams, and dates in backing index names was introduced.
     */
    public static final Version NEW_FEATURES_VERSION = Version.V_7_11_0;

    private final String name;
    private final TimestampField timeStampField;
    private final List<Index> indices;
    private final long generation;
    private final Map<String, Object> metadata;
    private final boolean hidden;
    private final boolean replicated;

    public DataStream(String name, TimestampField timeStampField, List<Index> indices, long generation, Map<String, Object> metadata) {
        this(name, timeStampField, indices, generation, metadata, false, false);
    }

    public DataStream(String name, TimestampField timeStampField, List<Index> indices, long generation, Map<String, Object> metadata,
                      boolean hidden, boolean replicated) {
        this.name = name;
        this.timeStampField = timeStampField;
        this.indices = Collections.unmodifiableList(indices);
        this.generation = generation;
        this.metadata = metadata;
        this.hidden = hidden;
        this.replicated = replicated;
        assert indices.size() > 0;
    }

    public DataStream(String name, TimestampField timeStampField, List<Index> indices) {
        this(name, timeStampField, indices, indices.size(), null);
    }

    public String getName() {
        return name;
    }

    public TimestampField getTimeStampField() {
        return timeStampField;
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

    public String nextWriteIndexName(Version minNodeVersion) {
        return DataStream.getDefaultBackingIndexName(getName(), getGeneration() + 1, minNodeVersion);
    }

    /**
     * Performs a rollover on a {@code DataStream} instance and returns a new instance containing
     * the updated list of backing indices and incremented generation.
     *
     * @param nextWriteIndex    The index that will be next write index after rollover has occurred.
     *                          This instance must have the same name that {@link #nextWriteIndexName(Version)} produces
     * @param minNodeVersion    minimum cluster node version
     * @return new {@code DataStream} instance with the rollover operation applied
     */
    public DataStream rollover(Index nextWriteIndex, Version minNodeVersion) {
        if (replicated) {
            throw new IllegalArgumentException("data stream [" + name + "] cannot be rolled over, " +
                "because it is a replicated data stream");
        }

        if (extractGenerationFromBackingIndex(nextWriteIndex.getName()) != generation + 1) {
            throw new IllegalArgumentException("expected next write index generation to be [" + generation + 1 +
                "], but was [" + extractGenerationFromBackingIndex(nextWriteIndex.getName()) + "]");
        }

        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.add(nextWriteIndex);
        return new DataStream(name, timeStampField, backingIndices, generation + 1, metadata, hidden, replicated);
    }

    private int extractGenerationFromBackingIndex(String backingIndexName) {
        int lastDashIndex = backingIndexName.lastIndexOf('-');
        assert lastDashIndex != -1;
        String generation = backingIndexName.substring(lastDashIndex + 1);
        try {
            // First assume that the format is: .ds-[name]-[generation]
            return Integer.parseInt(generation);
        } catch (NumberFormatException e) {
            // Then assume that the format is: .ds-[name]-[generation]-[random-snippet]
            int secondLastDashIndex = backingIndexName.lastIndexOf('-', lastDashIndex - 1);
            assert secondLastDashIndex != -1;
            generation = backingIndexName.substring(secondLastDashIndex + 1, lastDashIndex);
            return Integer.parseInt(generation);
        }
    }

    /**
     * Removes the specified backing index and returns a new {@code DataStream} instance with
     * the remaining backing indices.
     *
     * @param index the backing index to remove
     * @return new {@code DataStream} instance with the remaining backing indices
     */
    public DataStream removeBackingIndex(Index index) {
        List<Index> backingIndices = new ArrayList<>(indices);
        backingIndices.remove(index);
        assert backingIndices.size() == indices.size() - 1;
        return new DataStream(name, timeStampField, backingIndices, generation, metadata, hidden, replicated);
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
            throw new IllegalArgumentException(String.format(Locale.ROOT, "index [%s] is not part of data stream [%s] ",
                existingBackingIndex.getName(), name));
        }
        if (generation == (backingIndexPosition + 1)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "cannot replace backing index [%s] of data stream [%s] because " +
                "it is the write index", existingBackingIndex.getName(), name));
        }
        backingIndices.set(backingIndexPosition, newBackingIndex);
        return new DataStream(name, timeStampField, backingIndices, generation, metadata, hidden, replicated);
    }

    public DataStream promoteDataStream() {
        return new DataStream(name, timeStampField, indices, getGeneration(), metadata, hidden, false);
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
            timeStampField,
            reconciledIndices,
            generation,
            metadata == null ? null : new HashMap<>(metadata),
            hidden,
            replicated
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
        return getDefaultBackingIndexName(dataStreamName, generation, System.currentTimeMillis(), Version.CURRENT);
    }

    public static String getDefaultBackingIndexName(String dataStreamName, long generation, Version minNodeVersion) {
        return getDefaultBackingIndexName(dataStreamName, generation, System.currentTimeMillis(), minNodeVersion);
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
        return getDefaultBackingIndexName(dataStreamName, generation, epochMillis, Version.CURRENT);
    }

    /**
     * Generates the name of the index that conforms to the default naming convention for backing indices
     * on data streams given the specified data stream name, generation, and time.
     *
     * @param dataStreamName    name of the data stream
     * @param generation        generation of the data stream
     * @param epochMillis       creation time for the backing index
     * @param minNodeVersion    the minimum node version in the cluster, which controls whether legacy naming is used.
     * @return backing index name
     */
    public static String getDefaultBackingIndexName(String dataStreamName, long generation, long epochMillis, Version minNodeVersion) {
        if (minNodeVersion.onOrAfter(Version.V_8_0_0)) {
            return String.format(Locale.ROOT,
                BACKING_INDEX_PREFIX + "%s-%s-%06d-%s",
                dataStreamName,
                DATE_FORMATTER.formatMillis(epochMillis),
                generation,
                // Add a short user friendly snippet as suffix to backing index name.
                // The snippet doesn't have to be unique, as the backing index name already has a time component and
                // the backing index creation logic look will try again if it ends up in a pretty unique situation
                // where the backing index it tries to create already happens to exist.
                //
                // Not using UUIDs.randomBase64UUID(...) creates very long strings, which would make backing index
                // names very long and more difficult to read (at least more than it should be).
                randomizedSnippet());
        } else if (minNodeVersion.onOrAfter(NEW_FEATURES_VERSION)) {
            return String.format(Locale.ROOT,
                BACKING_INDEX_PREFIX + "%s-%s-%06d",
                dataStreamName,
                DATE_FORMATTER.formatMillis(epochMillis),
                generation);
        } else {
            return getLegacyDefaultBackingIndexName(dataStreamName, generation);
        }
    }

    public static String getLegacyDefaultBackingIndexName(String dataStreamName, long generation) {
        return String.format(Locale.ROOT, BACKING_INDEX_PREFIX + "%s-%06d", dataStreamName, generation);
    }

    public DataStream(StreamInput in) throws IOException {
        this(in.readString(), new TimestampField(in), in.readList(Index::new), in.readVLong(),
            in.getVersion().onOrAfter(NEW_FEATURES_VERSION) ? in.readMap(): null,
            in.getVersion().onOrAfter(NEW_FEATURES_VERSION) && in.readBoolean(),
            in.getVersion().onOrAfter(NEW_FEATURES_VERSION) && in.readBoolean());
    }

    public static Diff<DataStream> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(DataStream::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        timeStampField.writeTo(out);
        out.writeList(indices);
        out.writeVLong(generation);
        if (out.getVersion().onOrAfter(NEW_FEATURES_VERSION)) {
            out.writeMap(metadata);
            out.writeBoolean(hidden);
            out.writeBoolean(replicated);
        }
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField TIMESTAMP_FIELD_FIELD = new ParseField("timestamp_field");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField GENERATION_FIELD = new ParseField("generation");
    public static final ParseField METADATA_FIELD = new ParseField("_meta");
    public static final ParseField HIDDEN_FIELD = new ParseField("hidden");
    public static final ParseField REPLICATED_FIELD = new ParseField("replicated");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>("data_stream",
        args -> new DataStream((String) args[0], (TimestampField) args[1], (List<Index>) args[2], (Long) args[3],
            (Map<String, Object>) args[4], args[5] != null && (boolean) args[5], args[6] != null && (boolean) args[6]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), TimestampField.PARSER, TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Index.fromXContent(p), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), HIDDEN_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), REPLICATED_FIELD);
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(TIMESTAMP_FIELD_FIELD.getPreferredName(), timeStampField);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(GENERATION_FIELD.getPreferredName(), generation);
        if (metadata != null) {
            builder.field(METADATA_FIELD.getPreferredName(), metadata);
        }
        builder.field(HIDDEN_FIELD.getPreferredName(), hidden);
        builder.field(REPLICATED_FIELD.getPreferredName(), replicated);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStream that = (DataStream) o;
        return name.equals(that.name) &&
            timeStampField.equals(that.timeStampField) &&
            indices.equals(that.indices) &&
            generation == that.generation &&
            Objects.equals(metadata, that.metadata) &&
            hidden == that.hidden &&
            replicated == that.replicated;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timeStampField, indices, generation, metadata, hidden, replicated);
    }

    public static final class TimestampField implements Writeable, ToXContentObject {

        public static final String FIXED_TIMESTAMP_FIELD = "@timestamp";

        static ParseField NAME_FIELD = new ParseField("name");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<TimestampField, Void> PARSER = new ConstructingObjectParser<>(
            "timestamp_field",
            args -> new TimestampField((String) args[0])
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

    // Index names must be lowercase, so only randomly select characters from [0-9] and [a-z]:
    private static final char[] SNIPPET_CHAR_POOL = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

    private static String randomizedSnippet() {
        var random = Randomness.get();
        var snippetBuilder = new StringBuilder(6);
        for (int i = 0; i < 6; i++) {
            snippetBuilder.append(SNIPPET_CHAR_POOL[random.nextInt(SNIPPET_CHAR_POOL.length)]);
        }
        return snippetBuilder.toString();
    }
}
