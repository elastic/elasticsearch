/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DataStreamAlias implements SimpleDiffable<DataStreamAlias>, ToXContentFragment {

    public static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");
    public static final ParseField WRITE_DATA_STREAM_FIELD = new ParseField("write_data_stream");
    /*
     * Before 8.7.0, we incorrectly only kept one filter for all DataStreams in the DataStreamAlias. This field remains here so that we can
     * read old cluster states during an upgrade. We never write XContent with this field as of 8.7.0.
     */
    public static final ParseField OLD_FILTER_FIELD = new ParseField("filter");
    public static final ParseField FILTERS_FIELD = new ParseField("filters");
    private static final Logger logger = LogManager.getLogger(DataStreamAlias.class);

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamAlias, String> PARSER = new ConstructingObjectParser<>(
        "data_stream_alias",
        false,
        (args, name) -> {
            /*
             * If we are reading an older cluster state from disk we have to support reading in the single filter that was used before
             * 8.7.0. In this case the new dataStreamsToFilters map will be null. So we write a new dataStreamsToFilters using the existing
             * filter value for all DataStreams in order to carry forward the previously-existing behavior.
             */
            Map<String, CompressedXContent> dataStreamsToFilters = (Map<String, CompressedXContent>) args[3];
            CompressedXContent oldFilter = (CompressedXContent) args[2];
            List<String> dataStreamNames = (List<String>) args[0];
            if (dataStreamsToFilters == null && oldFilter != null && dataStreamNames != null) {
                logger.info(
                    "Reading in data stream alias [{}] with a pre-8.7.0-style data stream filter and using it for all data streams in "
                        + "the data stream alias",
                    name
                );
                dataStreamsToFilters = new HashMap<>();
                for (String dataStreamName : dataStreamNames) {
                    dataStreamsToFilters.put(dataStreamName, oldFilter);
                }
            }
            if (dataStreamsToFilters == null) {
                dataStreamsToFilters = Map.of();
            }
            return new DataStreamAlias(name, dataStreamNames, dataStreamsToFilters, (String) args[1]);
        }
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), DATA_STREAMS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), WRITE_DATA_STREAM_FIELD);
        // Note: This field is not used in 8.7.0 and higher:
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_EMBEDDED_OBJECT || p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new CompressedXContent(p.binaryValue());
            } else if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentBuilder builder = XContentFactory.jsonBuilder().map(p.mapOrdered());
                return new CompressedXContent(BytesReference.bytes(builder));
            } else {
                assert false : "unexpected token [" + p.currentToken() + " ]";
                return null;
            }
        }, OLD_FILTER_FIELD, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(HashMap::new, xContentParser -> {
            if (p.currentToken() == XContentParser.Token.VALUE_EMBEDDED_OBJECT || p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new CompressedXContent(p.binaryValue());
            } else if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentBuilder builder = XContentFactory.jsonBuilder().map(p.mapOrdered());
                return new CompressedXContent(BytesReference.bytes(builder));
            } else {
                assert false : "unexpected token [" + p.currentToken() + " ]";
                return null;
            }
        }), FILTERS_FIELD);
    }

    private final String name;
    private final List<String> dataStreams;
    private final String writeDataStream;
    // package-private for testing
    final Map<String, CompressedXContent> dataStreamToFilterMap;

    private DataStreamAlias(
        String name,
        List<String> dataStreams,
        Map<String, CompressedXContent> dataStreamsToFilters,
        String writeDataStream
    ) {
        this.name = Objects.requireNonNull(name);
        this.dataStreams = List.copyOf(dataStreams);
        this.writeDataStream = writeDataStream;
        this.dataStreamToFilterMap = new HashMap<>(dataStreamsToFilters);
        assert writeDataStream == null || dataStreams.contains(writeDataStream);
    }

    public DataStreamAlias(
        String name,
        List<String> dataStreams,
        String writeDataStream,
        Map<String, Map<String, Object>> dataStreamsToFilters
    ) {
        this(name, dataStreams, compressFiltersMap(dataStreamsToFilters), writeDataStream);
    }

    private static Map<String, CompressedXContent> compressFiltersMap(Map<String, Map<String, Object>> dataStreamToUncompressedFilterMap) {
        if (dataStreamToUncompressedFilterMap == null) {
            return Map.of();
        }
        return dataStreamToUncompressedFilterMap.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> compress(entry.getValue())));
    }

    private static CompressedXContent compress(Map<String, Object> filterAsMap) {
        if (filterAsMap == null) {
            return null;
        }

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().map(filterAsMap);
            return new CompressedXContent(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<String, Object> decompress(CompressedXContent filter) {
        String filterAsString = filter.string();
        return XContentHelper.convertToMap(XContentFactory.xContent(filterAsString), filterAsString, true);
    }

    public DataStreamAlias(StreamInput in) throws IOException {
        this.name = in.readString();
        this.dataStreams = in.readStringList();
        this.writeDataStream = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            this.dataStreamToFilterMap = in.readMap(CompressedXContent::readCompressedString);
        } else {
            this.dataStreamToFilterMap = new HashMap<>();
            CompressedXContent filter = in.readBoolean() ? CompressedXContent.readCompressedString(in) : null;
            if (filter != null) {
                /*
                 * Here we're reading in a DataStreamAlias from before 8.7.0, which did not correctly associate filters with DataStreams.
                 * So we associated the same filter with all DataStreams in the alias to replicate the old behavior.
                 */
                for (String dataStream : dataStreams) {
                    dataStreamToFilterMap.put(dataStream, filter);
                }
            }
        }
    }

    /**
     * Returns the name of this data stream alias.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the data streams that are referenced
     */
    public List<String> getDataStreams() {
        return dataStreams;
    }

    /**
     * Returns the write data stream this data stream alias is referring to.
     * Write requests targeting this instance will resolve the write index
     * of the write data stream this alias is referring to.
     *
     * Note that the write data stream is also included in {@link #getDataStreams()}.
     */
    public String getWriteDataStream() {
        return writeDataStream;
    }

    public CompressedXContent getFilter(String dataStreamName) {
        return dataStreamToFilterMap.get(dataStreamName);
    }

    public boolean filteringRequired() {
        return dataStreamToFilterMap.isEmpty() == false;
    }

    /**
     * Returns a new {@link DataStreamAlias} instance with the provided data stream name added to it as a new member.
     * If the provided isWriteDataStream is set to <code>true</code> then the provided data stream is also set as write data stream.
     * If the provided isWriteDataStream is set to <code>false</code> and the provided data stream is also the write data stream of
     * this instance then the returned data stream alias instance's write data stream is unset.
     * If the provided filter is the same as the filter of this alias then this instance isn't updated, otherwise it is updated.
     *
     * The same instance is returned if the attempted addition of the provided data stream didn't change this instance.
     */
    public DataStreamAlias update(String dataStream, Boolean isWriteDataStream, Map<String, Object> filterAsMap) {
        String writeDataStream = this.writeDataStream;
        if (isWriteDataStream != null) {
            if (isWriteDataStream) {
                writeDataStream = dataStream;
            } else {
                if (dataStream.equals(writeDataStream)) {
                    writeDataStream = null;
                }
            }
        }

        boolean filterUpdated;
        if (filterAsMap != null) {
            CompressedXContent previousFilter = dataStreamToFilterMap.get(dataStream);
            if (previousFilter == null) {
                filterUpdated = true;
            } else {
                filterUpdated = filterAsMap.equals(decompress(previousFilter)) == false;
            }
        } else {
            filterUpdated = false;
        }

        Set<String> dataStreams = new HashSet<>(this.dataStreams);
        boolean added = dataStreams.add(dataStream);
        if (added || Objects.equals(this.writeDataStream, writeDataStream) == false || filterUpdated) {
            Map<String, CompressedXContent> newDataStreamToFilterMap = new HashMap<>(dataStreamToFilterMap);
            if (filterAsMap != null) {
                newDataStreamToFilterMap.put(dataStream, compress(filterAsMap));
            }
            return new DataStreamAlias(name, List.copyOf(dataStreams), newDataStreamToFilterMap, writeDataStream);
        } else {
            return this;
        }
    }

    /**
     * Returns a {@link DataStreamAlias} instance based on this instance but with the specified data stream no longer referenced.
     * Returns <code>null</code> if because of the removal of the provided data stream name a new instance wouldn't reference to
     * any data stream. The same instance is returned if the attempted removal of the provided data stream didn't change this instance.
     */
    public DataStreamAlias removeDataStream(String dataStream) {
        Set<String> dataStreams = new HashSet<>(this.dataStreams);
        boolean removed = dataStreams.remove(dataStream);
        if (removed == false) {
            return this;
        }

        if (dataStreams.isEmpty()) {
            return null;
        } else {
            String writeDataStream = this.writeDataStream;
            if (dataStream.equals(writeDataStream)) {
                writeDataStream = null;
            }
            return new DataStreamAlias(name, List.copyOf(dataStreams), dataStreamToFilterMap, writeDataStream);
        }
    }

    /**
     * Returns a new {@link DataStreamAlias} instance that contains a new intersection
     * of data streams from this instance and the provided filter.
     *
     * The write data stream gets set to null in the returned instance if the write
     * data stream no longer appears in the intersection.
     */
    public DataStreamAlias intersect(Predicate<String> filter) {
        List<String> intersectingDataStreams = this.dataStreams.stream().filter(filter).toList();
        String writeDataStream = this.writeDataStream;
        if (intersectingDataStreams.contains(writeDataStream) == false) {
            writeDataStream = null;
        }
        return new DataStreamAlias(this.name, intersectingDataStreams, this.dataStreamToFilterMap, writeDataStream);
    }

    /**
     * Performs alias related restore operations for this instance as part of the entire restore operation.
     *
     * If a previous instance is provided then it merges the data streams referenced in this instance and
     * the previous instance. If this instance doesn't have a write data stream then the write index of
     * the other data stream becomes the write data stream of the returned instance. If both this and
     * previous instances have a write data stream then these write data streams need to be the same.
     *
     * If a renamePattern and renameReplacement is provided then data streams this instance is referring to
     * are renamed. Assuming that those data streams match with the specified renamePattern.
     *
     * @param previous          Optionally, the alias instance that this alias instance is replacing.
     * @param renamePattern     Optionally, the pattern that is required to match to rename data streams this alias is referring to.
     * @param renameReplacement Optionally, the replacement used to rename data streams this alias is referring to.
     * @return a new alias instance that can be applied in the cluster state
     */
    public DataStreamAlias restore(DataStreamAlias previous, String renamePattern, String renameReplacement) {
        Set<String> mergedDataStreams = previous != null ? new HashSet<>(previous.getDataStreams()) : new HashSet<>();

        String writeDataStream = this.writeDataStream;
        if (renamePattern != null && renameReplacement != null) {
            this.dataStreams.stream().map(s -> s.replaceAll(renamePattern, renameReplacement)).forEach(mergedDataStreams::add);
            if (writeDataStream != null) {
                writeDataStream = writeDataStream.replaceAll(renamePattern, renameReplacement);
            }
        } else {
            mergedDataStreams.addAll(this.dataStreams);
        }

        if (previous != null) {
            if (writeDataStream != null && previous.getWriteDataStream() != null) {
                String previousWriteDataStream = previous.getWriteDataStream();
                if (renamePattern != null && renameReplacement != null) {
                    previousWriteDataStream = previousWriteDataStream.replaceAll(renamePattern, renameReplacement);
                }
                if (writeDataStream.equals(previousWriteDataStream) == false) {
                    throw new IllegalArgumentException(
                        "cannot merge alias ["
                            + name
                            + "], write data stream of this ["
                            + writeDataStream
                            + "] and write data stream of other ["
                            + previous.getWriteDataStream()
                            + "] are different"
                    );
                }
            } else if (writeDataStream == null && previous.getWriteDataStream() != null) {
                // The write alias should exist in the set of merged data streams. It shouldn't be possible to construct an alias with
                // a write data stream, that doesn't exist in the list of data streams.
                assert mergedDataStreams.contains(previous.getWriteDataStream());
                writeDataStream = previous.getWriteDataStream();
            }
        }

        return new DataStreamAlias(this.name, List.copyOf(mergedDataStreams), dataStreamToFilterMap, writeDataStream);
    }

    public static Diff<DataStreamAlias> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamAlias::new, in);
    }

    public static DataStreamAlias fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        String name = parser.currentName();
        return PARSER.parse(parser, name);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.stringListField(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
        if (writeDataStream != null) {
            builder.field(WRITE_DATA_STREAM_FIELD.getPreferredName(), writeDataStream);
        }
        boolean binary = params.paramAsBoolean("binary", false);
        builder.startObject("filters");
        for (Map.Entry<String, CompressedXContent> entry : dataStreamToFilterMap.entrySet()) {
            if (binary) {
                builder.field(entry.getKey(), entry.getValue().compressed());
            } else {
                builder.field(entry.getKey(), XContentHelper.convertToMap(entry.getValue().uncompressed(), true).v2());
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(dataStreams);
        out.writeOptionalString(writeDataStream);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeMap(dataStreamToFilterMap, StreamOutput::writeString, (out1, filter) -> filter.writeTo(out1));
        } else {
            if (dataStreamToFilterMap.isEmpty()) {
                out.writeBoolean(false);
            } else {
                /*
                 * TransportVersions before 8.7 incorrectly only allowed a single filter for all datastreams,
                 * and randomly dropped all others. We replicate that buggy behavior here if we have to write
                 * to an older node because there is no way to send multipole filters to an older node.
                 */
                dataStreamToFilterMap.values().iterator().next().writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStreamAlias that = (DataStreamAlias) o;
        return Objects.equals(name, that.name)
            && Objects.equals(dataStreams, that.dataStreams)
            && Objects.equals(writeDataStream, that.writeDataStream)
            && Objects.equals(dataStreamToFilterMap, that.dataStreamToFilterMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataStreams, writeDataStream, dataStreamToFilterMap);
    }

    @Override
    public String toString() {
        return "DataStreamAlias{"
            + "name='"
            + name
            + '\''
            + ", dataStreams="
            + dataStreams
            + ", writeDataStream='"
            + writeDataStream
            + '\''
            + ", dataStreamToFilterMap="
            + dataStreamToFilterMap.keySet()
                .stream()
                .map(key -> key + "=" + dataStreamToFilterMap.get(key))
                .collect(Collectors.joining(", ", "{", "}"))
            + '}';
    }
}
