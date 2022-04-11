/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.List;

/**
 * A {@link LookupField} is an **unresolved** fetch field whose values will be resolved later
 * in the fetch phase on the coordinating node.
 *
 * @see org.elasticsearch.index.mapper.LookupRuntimeFieldType
 */
public record LookupField(String targetIndex, QueryBuilder query, List<FieldAndFormat> fetchFields, int size) implements Writeable {

    public LookupField(StreamInput in) throws IOException {
        this(in.readString(), in.readNamedWriteable(QueryBuilder.class), in.readList(FieldAndFormat::new), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(targetIndex);
        out.writeNamedWriteable(query);
        out.writeCollection(fetchFields);
        out.writeVInt(size);
    }

    public SearchRequest toSearchRequest(String clusterAlias) {
        final SearchSourceBuilder source = new SearchSourceBuilder().query(query).trackScores(false).size(size).fetchSource(false);
        fetchFields.forEach(source::fetchField);
        return new SearchRequest().source(source).indices(RemoteClusterAware.buildRemoteIndexName(clusterAlias, targetIndex));
    }
}
