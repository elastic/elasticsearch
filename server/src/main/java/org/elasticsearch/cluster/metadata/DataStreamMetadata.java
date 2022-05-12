/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

/**
 * Custom {@link Metadata} implementation for storing a map of {@link DataStream}s and their names.
 */
public class DataStreamMetadata implements Metadata.Custom {

    public static final String TYPE = "data_stream";

    public static final DataStreamMetadata EMPTY = new DataStreamMetadata(ImmutableOpenMap.of(), ImmutableOpenMap.of());
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField DATA_STREAM_ALIASES = new ParseField("data_stream_aliases");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false, args -> {
        ImmutableOpenMap<String, DataStream> dataStreams = (ImmutableOpenMap<String, DataStream>) args[0];
        ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases = (ImmutableOpenMap<String, DataStreamAlias>) args[1];
        if (dataStreamAliases == null) {
            dataStreamAliases = ImmutableOpenMap.of();
        }
        return new DataStreamMetadata(dataStreams, dataStreamAliases);
    });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            ImmutableOpenMap.Builder<String, DataStream> dataStreams = ImmutableOpenMap.builder();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                dataStreams.put(name, DataStream.fromXContent(p));
            }
            return dataStreams.build();
        }, DATA_STREAM);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            ImmutableOpenMap.Builder<String, DataStreamAlias> dataStreams = ImmutableOpenMap.builder();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                DataStreamAlias alias = DataStreamAlias.fromXContent(p);
                dataStreams.put(alias.getName(), alias);
            }
            return dataStreams.build();
        }, DATA_STREAM_ALIASES);
    }

    private final ImmutableOpenMap<String, DataStream> dataStreams;
    private final ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases;

    public DataStreamMetadata(
        ImmutableOpenMap<String, DataStream> dataStreams,
        ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases
    ) {
        this.dataStreams = dataStreams;
        this.dataStreamAliases = dataStreamAliases;
    }

    public DataStreamMetadata(StreamInput in) throws IOException {
        this(
            in.readImmutableOpenMap(StreamInput::readString, DataStream::new),
            in.readImmutableOpenMap(StreamInput::readString, DataStreamAlias::new)
        );
    }

    public DataStreamMetadata withAddedDatastream(DataStream datastream) {
        final String name = datastream.getName();
        final DataStream existing = dataStreams.get(name);
        if (datastream.equals(existing)) {
            return this;
        }
        return new DataStreamMetadata(ImmutableOpenMap.builder(dataStreams).fPut(name, datastream).build(), dataStreamAliases);
    }

    public ImmutableOpenMap<String, DataStream> dataStreams() {
        return this.dataStreams;
    }

    public ImmutableOpenMap<String, DataStreamAlias> getDataStreamAliases() {
        return dataStreamAliases;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new DataStreamMetadata.DataStreamMetadataDiff((DataStreamMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new DataStreamMetadata.DataStreamMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public boolean isRestorable() {
        // this metadata is written to the snapshot, however it uses custom logic for restoring
        return false;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_7_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.dataStreams, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
        out.writeMap(this.dataStreamAliases, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    public static DataStreamMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.xContentValuesMap(DATA_STREAM.getPreferredName(), dataStreams);
        builder.startObject(DATA_STREAM_ALIASES.getPreferredName());
        for (Map.Entry<String, DataStreamAlias> dataStream : dataStreamAliases.entrySet()) {
            dataStream.getValue().toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.dataStreams, dataStreamAliases);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataStreamMetadata other = (DataStreamMetadata) obj;
        return Objects.equals(this.dataStreams, other.dataStreams) && Objects.equals(this.dataStreamAliases, other.dataStreamAliases);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class DataStreamMetadataDiff implements NamedDiff<Metadata.Custom> {

        private static final DiffableUtils.DiffableValueReader<String, DataStream> DS_DIFF_READER = new DiffableUtils.DiffableValueReader<>(
            DataStream::new,
            DataStream::readDiffFrom
        );

        private static final DiffableUtils.DiffableValueReader<String, DataStreamAlias> ALIAS_DIFF_READER =
            new DiffableUtils.DiffableValueReader<>(DataStreamAlias::new, DataStreamAlias::readDiffFrom);

        final DiffableUtils.MapDiff<String, DataStream, ImmutableOpenMap<String, DataStream>> dataStreamDiff;
        final DiffableUtils.MapDiff<String, DataStreamAlias, ImmutableOpenMap<String, DataStreamAlias>> dataStreamAliasDiff;

        DataStreamMetadataDiff(DataStreamMetadata before, DataStreamMetadata after) {
            this.dataStreamDiff = DiffableUtils.diff(before.dataStreams, after.dataStreams, DiffableUtils.getStringKeySerializer());
            this.dataStreamAliasDiff = DiffableUtils.diff(
                before.dataStreamAliases,
                after.dataStreamAliases,
                DiffableUtils.getStringKeySerializer()
            );
        }

        DataStreamMetadataDiff(StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), DS_DIFF_READER);
            this.dataStreamAliasDiff = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ALIAS_DIFF_READER
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new DataStreamMetadata(
                ImmutableOpenMap.<String, DataStream>builder()
                    .putAllFromMap(dataStreamDiff.apply(((DataStreamMetadata) part).dataStreams))
                    .build(),
                dataStreamAliasDiff != null
                    ? dataStreamAliasDiff.apply(((DataStreamMetadata) part).dataStreamAliases)
                    : ImmutableOpenMap.of()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
            dataStreamAliasDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_7_7_0;
        }
    }
}
