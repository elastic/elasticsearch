/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetAliasesResponse extends ActionResponse {

    private final Map<String, List<AliasMetadata>> aliases;
    private final Map<String, List<DataStreamAlias>> dataStreamAliases;

    public GetAliasesResponse(Map<String, List<AliasMetadata>> aliases, Map<String, List<DataStreamAlias>> dataStreamAliases) {
        this.aliases = aliases;
        this.dataStreamAliases = dataStreamAliases;
    }

    public Map<String, List<AliasMetadata>> getAliases() {
        return aliases;
    }

    public Map<String, List<DataStreamAlias>> getDataStreamAliases() {
        return dataStreamAliases;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
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
        return Objects.equals(aliases, that.aliases) && Objects.equals(dataStreamAliases, that.dataStreamAliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases, dataStreamAliases);
    }
}
