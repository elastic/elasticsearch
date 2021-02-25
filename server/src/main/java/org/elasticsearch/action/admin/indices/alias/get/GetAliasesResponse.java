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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetAliasesResponse extends ActionResponse {

    private final ImmutableOpenMap<String, List<AliasMetadata>> aliases;

    public GetAliasesResponse(ImmutableOpenMap<String, List<AliasMetadata>> aliases) {
        this.aliases = aliases;
    }

    public GetAliasesResponse(StreamInput in) throws IOException {
        super(in);
        aliases = in.readImmutableMap(StreamInput::readString, i -> i.readList(AliasMetadata::new));
    }

    public ImmutableOpenMap<String, List<AliasMetadata>> getAliases() {
        return aliases;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(aliases, StreamOutput::writeString, StreamOutput::writeList);
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
        return Objects.equals(aliases, that.aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases);
    }
}
