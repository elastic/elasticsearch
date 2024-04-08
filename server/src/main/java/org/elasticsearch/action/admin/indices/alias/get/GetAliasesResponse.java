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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.UpdateForV9;

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

    /**
     * NB prior to 8.12 get-aliases was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until we no
     * longer need to support calling this action remotely.
     */
    @UpdateForV9 // replace this implementation with TransportAction.localOnly()
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(aliases, StreamOutput::writeCollection);
        out.writeMap(dataStreamAliases, StreamOutput::writeCollection);
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
