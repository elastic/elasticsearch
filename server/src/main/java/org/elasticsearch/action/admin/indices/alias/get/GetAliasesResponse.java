/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetAliasesResponse extends ActionResponse {

    private final ImmutableOpenMap<String, List<AliasMetadata>> aliases;
    private final Map<String, List<DataStreamAlias>> dataStreamAliases;

    public GetAliasesResponse(ImmutableOpenMap<String, List<AliasMetadata>> aliases, Map<String, List<DataStreamAlias>> dataStreamAliases) {
        this.aliases = aliases;
        this.dataStreamAliases = dataStreamAliases;
    }

    public GetAliasesResponse(StreamInput in) throws IOException {
        super(in);
        aliases = in.readImmutableMap(StreamInput::readString, i -> i.readList(AliasMetadata::new));
        dataStreamAliases = in.getVersion().onOrAfter(DataStreamMetadata.DATA_STREAM_ALIAS_VERSION) ?
            in.readMap(StreamInput::readString, in1 -> in1.readList(DataStreamAlias::new)) : Map.of();
    }

    public ImmutableOpenMap<String, List<AliasMetadata>> getAliases() {
        return aliases;
    }

    public Map<String, List<DataStreamAlias>> getDataStreamAliases() {
        return dataStreamAliases;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(aliases, StreamOutput::writeString, StreamOutput::writeList);
        if (out.getVersion().onOrAfter(DataStreamMetadata.DATA_STREAM_ALIAS_VERSION)) {
            out.writeMap(dataStreamAliases, StreamOutput::writeString, StreamOutput::writeList);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetAliasesResponse that = (GetAliasesResponse) o;
        return Objects.equals(aliases, that.aliases) &&
            Objects.equals(dataStreamAliases, that.dataStreamAliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases, dataStreamAliases);
    }
}
