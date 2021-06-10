/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DataStreamAlias extends AbstractDiffable<DataStreamAlias> implements ToXContentFragment {

    public static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");
    public static final ParseField WRITE_DATA_STREAM_FIELD = new ParseField("write_data_stream");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamAlias, String> PARSER = new ConstructingObjectParser<>(
        "data_stream_alias",
        false,
        (args, name) -> new DataStreamAlias(name, (List<String>) args[0], (String) args[1])
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), DATA_STREAMS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), WRITE_DATA_STREAM_FIELD);
    }

    private final String name;
    private final List<String> dataStreams;
    private final String writeDataStream;

    public DataStreamAlias(String name, List<String> dataStreams, String writeDataStream) {
        this.name = Objects.requireNonNull(name);
        this.dataStreams = List.copyOf(dataStreams);
        this.writeDataStream = writeDataStream;
        assert writeDataStream == null || dataStreams.contains(writeDataStream);
    }

    public DataStreamAlias(StreamInput in) throws IOException {
        this.name = in.readString();
        this.dataStreams = in.readStringList();
        this.writeDataStream = in.readOptionalString();
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

    /**
     * Returns a new {@link DataStreamAlias} instance with the provided data stream name added to it as a new member.
     * If the provided isWriteDataStream is set to <code>true</code> then the provided data stream is also set as write data stream.
     * If the provided isWriteDataStream is set to <code>false</code> and the provided data stream is also the write data stream of
     * this instance then the returned data stream alias instance's write data stream is unset.
     *
     * The same instance is returned if the attempted addition of the provided data stream didn't change this instance.
     */
    public DataStreamAlias addDataStream(String dataStream, Boolean isWriteDataStream) {
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

        Set<String> dataStreams = new HashSet<>(this.dataStreams);
        boolean added = dataStreams.add(dataStream);
        if (added || Objects.equals(this.writeDataStream, writeDataStream) == false) {
            return new DataStreamAlias(name, List.copyOf(dataStreams), writeDataStream);
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
            return new DataStreamAlias(name, List.copyOf(dataStreams), writeDataStream);
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
        List<String> intersectingDataStreams = this.dataStreams.stream()
            .filter(filter)
            .collect(Collectors.toList());
        String writeDataStream = this.writeDataStream;
        if (intersectingDataStreams.contains(writeDataStream) == false) {
            writeDataStream = null;
        }
        return new DataStreamAlias(this.name, intersectingDataStreams, writeDataStream);
    }

    /**
     * Returns a new {@link DataStreamAlias} instance containing data streams referenced in this instance
     * and the other instance. If this instance doesn't have a write data stream then the write index of
     * the other data stream becomes the write data stream of the returned instance.
     */
    public DataStreamAlias merge(DataStreamAlias other) {
        Set<String> mergedDataStreams = new HashSet<>(other.getDataStreams());
        mergedDataStreams.addAll(this.getDataStreams());

        String writeDataStream = this.writeDataStream;
        if (writeDataStream == null) {
            if (other.getWriteDataStream() != null && mergedDataStreams.contains(other.getWriteDataStream())) {
                writeDataStream = other.getWriteDataStream();
            }
        }

        return new DataStreamAlias(this.name, List.copyOf(mergedDataStreams), writeDataStream);
    }

    /**
     * Returns a new instance with potentially renamed data stream names and write data stream name.
     * If a data stream name matches with the provided rename pattern then it is renamed according
     * to the provided rename replacement.
     */
    public DataStreamAlias renameDataStreams(String renamePattern, String renameReplacement) {
        List<String> renamedDataStreams = this.dataStreams.stream()
            .map(s -> s.replaceAll(renamePattern, renameReplacement))
            .collect(Collectors.toList());
        String writeDataStream = this.writeDataStream;
        if (writeDataStream != null) {
            writeDataStream = writeDataStream.replaceAll(renamePattern, renameReplacement);
        }
        return new DataStreamAlias(this.name, renamedDataStreams, writeDataStream);
    }

    public static Diff<DataStreamAlias> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(DataStreamAlias::new, in);
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
        builder.field(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
        if (writeDataStream != null) {
            builder.field(WRITE_DATA_STREAM_FIELD.getPreferredName(), writeDataStream);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(dataStreams);
        out.writeOptionalString(writeDataStream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStreamAlias that = (DataStreamAlias) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(dataStreams, that.dataStreams) &&
            Objects.equals(writeDataStream, that.writeDataStream);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataStreams, writeDataStream);
    }
}
