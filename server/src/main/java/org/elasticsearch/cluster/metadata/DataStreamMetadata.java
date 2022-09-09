/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Custom {@link Metadata} implementation for storing a map of {@link DataStream}s and their names.
 */
public class DataStreamMetadata implements Metadata.Custom {

    public static final String TYPE = "data_stream";

    public static final DataStreamMetadata EMPTY = new DataStreamMetadata(Map.of(), Map.of());
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField DATA_STREAM_ALIASES = new ParseField("data_stream_aliases");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false, args -> {
        Map<String, DataStream> dataStreams = (Map<String, DataStream>) args[0];
        Map<String, DataStreamAlias> dataStreamAliases = (Map<String, DataStreamAlias>) args[1];
        if (dataStreamAliases == null) {
            dataStreamAliases = Map.of();
        }
        return new DataStreamMetadata(dataStreams, dataStreamAliases);
    });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DataStream> dataStreams = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                dataStreams.put(name, DataStream.fromXContent(p));
            }
            return Collections.unmodifiableMap(dataStreams);
        }, DATA_STREAM);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            Map<String, DataStreamAlias> dataStreams = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                DataStreamAlias alias = DataStreamAlias.fromXContent(p);
                dataStreams.put(alias.getName(), alias);
            }
            return Collections.unmodifiableMap(dataStreams);
        }, DATA_STREAM_ALIASES);
    }

    private final Map<String, DataStream> dataStreams;
    private final Map<String, DataStreamAlias> dataStreamAliases;

    public DataStreamMetadata(Map<String, DataStream> dataStreams, Map<String, DataStreamAlias> dataStreamAliases) {
        this.dataStreams = Map.copyOf(dataStreams);
        this.dataStreamAliases = Map.copyOf(dataStreamAliases);
    }

    public DataStreamMetadata(StreamInput in) throws IOException {
        this(
            in.readImmutableMap(StreamInput::readString, DataStream::new),
            in.readImmutableMap(StreamInput::readString, DataStreamAlias::new)
        );
    }

    public DataStreamMetadata withAddedDatastream(DataStream datastream) {
        final String name = datastream.getName();
        final DataStream existing = dataStreams.get(name);
        if (datastream.equals(existing)) {
            return this;
        }
        Map<String, DataStream> streams = new HashMap<>(dataStreams);
        streams.put(name, datastream);
        return new DataStreamMetadata(streams, dataStreamAliases);
    }

    public DataStreamMetadata withAlias(String aliasName, String dataStream, Boolean isWriteDataStream, String filter) {
        if (dataStreams.containsKey(dataStream) == false) {
            throw new IllegalArgumentException("alias [" + aliasName + "] refers to a non existing data stream [" + dataStream + "]");
        }

        Map<String, Object> filterAsMap;
        if (filter != null) {
            filterAsMap = XContentHelper.convertToMap(XContentFactory.xContent(filter), filter, true);
        } else {
            filterAsMap = null;
        }

        DataStreamAlias alias = dataStreamAliases.get(aliasName);
        if (alias == null) {
            String writeDataStream = isWriteDataStream != null && isWriteDataStream ? dataStream : null;
            alias = new DataStreamAlias(aliasName, List.of(dataStream), writeDataStream, filterAsMap);
        } else {
            DataStreamAlias copy = alias.update(dataStream, isWriteDataStream, filterAsMap);
            if (copy == alias) {
                return this;
            }
            alias = copy;
        }

        Map<String, DataStreamAlias> aliases = new HashMap<>(dataStreamAliases);
        aliases.put(aliasName, alias);
        return new DataStreamMetadata(dataStreams, aliases);
    }

    public DataStreamMetadata withRemovedDataStream(String name) {
        Map<String, DataStream> existingDataStreams = new HashMap<>(dataStreams);
        Map<String, DataStreamAlias> existingDataStreamAliases = new HashMap<>(dataStreamAliases);
        existingDataStreams.remove(name);

        Set<String> aliasesToDelete = new HashSet<>();
        List<DataStreamAlias> aliasesToUpdate = new ArrayList<>();
        for (var alias : dataStreamAliases.values()) {
            DataStreamAlias copy = alias.removeDataStream(name);
            if (copy != null) {
                if (copy == alias) {
                    continue;
                }
                aliasesToUpdate.add(copy);
            } else {
                aliasesToDelete.add(alias.getName());
            }
        }
        for (DataStreamAlias alias : aliasesToUpdate) {
            existingDataStreamAliases.put(alias.getName(), alias);
        }
        for (String aliasToDelete : aliasesToDelete) {
            existingDataStreamAliases.remove(aliasToDelete);
        }
        return new DataStreamMetadata(existingDataStreams, existingDataStreamAliases);
    }

    public DataStreamMetadata withRemovedAlias(String aliasName, String dataStreamName, boolean mustExist) {
        Map<String, DataStreamAlias> dataStreamAliases = new HashMap<>(this.dataStreamAliases);

        DataStreamAlias existing = dataStreamAliases.get(aliasName);
        if (mustExist && existing == null) {
            throw new ResourceNotFoundException("alias [" + aliasName + "] doesn't exist");
        } else if (existing == null) {
            return this;
        }
        DataStreamAlias copy = existing.removeDataStream(dataStreamName);
        if (copy == existing) {
            return this;
        }
        if (copy != null) {
            dataStreamAliases.put(aliasName, copy);
        } else {
            dataStreamAliases.remove(aliasName);
        }
        return new DataStreamMetadata(dataStreams, dataStreamAliases);
    }

    public Map<String, DataStream> dataStreams() {
        return this.dataStreams;
    }

    public Map<String, DataStreamAlias> getDataStreamAliases() {
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

        final DiffableUtils.MapDiff<String, DataStream, Map<String, DataStream>> dataStreamDiff;
        final DiffableUtils.MapDiff<String, DataStreamAlias, Map<String, DataStreamAlias>> dataStreamAliasDiff;

        DataStreamMetadataDiff(DataStreamMetadata before, DataStreamMetadata after) {
            this.dataStreamDiff = DiffableUtils.diff(before.dataStreams, after.dataStreams, DiffableUtils.getStringKeySerializer());
            this.dataStreamAliasDiff = DiffableUtils.diff(
                before.dataStreamAliases,
                after.dataStreamAliases,
                DiffableUtils.getStringKeySerializer()
            );
        }

        DataStreamMetadataDiff(StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), DS_DIFF_READER);
            this.dataStreamAliasDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), ALIAS_DIFF_READER);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new DataStreamMetadata(
                dataStreamDiff.apply(((DataStreamMetadata) part).dataStreams),
                dataStreamAliasDiff.apply(((DataStreamMetadata) part).dataStreamAliases)
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
