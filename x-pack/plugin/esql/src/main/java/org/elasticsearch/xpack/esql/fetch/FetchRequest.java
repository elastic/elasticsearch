/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Asks one data node for the fetched columns of documents that survived a cut. For each shard it names the reader context
 * the query phase kept open, whether the node frees it once it answered, and the documents to load, and it names the
 * attributes to load for them.
 * <p>
 * It names the indices of its shards, or the aliases the query reached them through, without wildcards, like the shard
 * requests of {@code _search}. Security authorizes the request for all of them, so a user who can no longer read one of
 * the indices gets the whole request refused.
 */
public final class FetchRequest extends AbstractTransportRequest implements IndicesRequest {
    static final TransportVersion ESQL_FETCH = TransportVersion.fromName("esql_fetch_action");

    /**
     * The documents to load from one shard, sorted by segment and then by doc, without duplicates. The node returns their
     * rows in this order.
     *
     * @param contextId      the reader context the query phase kept open for the shard
     * @param freeAfterFetch whether the node frees the context once it answered, because this request reads it for the
     *                       last time, like the fetch phase of {@code _search} frees the context it read
     */
    public record ShardDocs(ShardId shardId, ShardSearchContextId contextId, boolean freeAfterFetch, int[] segments, int[] docs)
        implements
            Writeable {
        public ShardDocs {
            FetchDocsSourceOperator.ShardDocs.checkSorted(segments, docs);
        }

        static ShardDocs readFrom(StreamInput in) throws IOException {
            return new ShardDocs(new ShardId(in), new ShardSearchContextId(in), in.readBoolean(), in.readVIntArray(), in.readVIntArray());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            contextId.writeTo(out);
            out.writeBoolean(freeAfterFetch);
            out.writeVIntArray(segments);
            out.writeVIntArray(docs);
        }

        public int docCount() {
            return docs.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShardDocs that = (ShardDocs) o;
            return shardId.equals(that.shardId)
                && contextId.equals(that.contextId)
                && freeAfterFetch == that.freeAfterFetch
                && Arrays.equals(segments, that.segments)
                && Arrays.equals(docs, that.docs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, contextId, freeAfterFetch, Arrays.hashCode(segments), Arrays.hashCode(docs));
        }

        @Override
        public String toString() {
            return "ShardDocs[" + shardId + ", " + contextId + ", freeAfterFetch=" + freeAfterFetch + ", docs=" + docs.length + "]";
        }
    }

    private final String sessionId;
    private final String clusterAlias;
    private final String[] indices;
    private final IndicesOptions indicesOptions;
    private final List<ShardDocs> shards;
    private final Configuration configuration;
    private final List<Attribute> attributes;

    /**
     * @param indices    the indices of the shards, or the aliases the query reached them through, without wildcards
     * @param attributes the attributes to load, the columns of the response pages
     */
    public FetchRequest(
        String sessionId,
        String clusterAlias,
        OriginalIndices indices,
        List<ShardDocs> shards,
        Configuration configuration,
        List<Attribute> attributes
    ) {
        this.sessionId = sessionId;
        this.clusterAlias = clusterAlias;
        this.indices = indices.indices();
        this.indicesOptions = indices.indicesOptions();
        this.shards = List.copyOf(shards);
        // the fetch reads no lookup tables, so the request doesn't ship them
        this.configuration = configuration.withoutTables();
        this.attributes = checkAttributes(attributes);
    }

    public FetchRequest(StreamInput in) throws IOException {
        this(in, null);
    }

    /**
     * @param idMapper {@code null} in production. Tests map the ids of the attributes so a copy equals the original.
     */
    FetchRequest(StreamInput in, @Nullable PlanStreamInput.NameIdMapper idMapper) throws IOException {
        super(in);
        this.sessionId = in.readString();
        this.clusterAlias = in.readString();
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.shards = in.readCollectionAsImmutableList(ShardDocs::readFrom);
        // without tables the configuration holds no blocks, so this factory never allocates
        this.configuration = new Configuration(
            new BlockStreamInput(
                in,
                BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST)).build()
            )
        );
        this.attributes = checkAttributes(
            new PlanStreamInput(in, in.namedWriteableRegistry(), configuration, idMapper).readNamedWriteableCollectionAsList(
                Attribute.class
            )
        );
    }

    /**
     * A {@code _doc} column can't leave the node, and its shard references would leak to the coordinator.
     */
    private static List<Attribute> checkAttributes(List<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            if (attribute.dataType() == DataType.DOC_DATA_TYPE) {
                throw new IllegalArgumentException("a fetch can't load [" + attribute + "]");
            }
        }
        return List.copyOf(attributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_FETCH) == false) {
            // only nodes that sent documents to the coordinator in the query phase get fetch requests
            throw new IllegalStateException("can't send a fetch request to a node on [" + out.getTransportVersion() + "]");
        }
        super.writeTo(out);
        out.writeString(sessionId);
        out.writeString(clusterAlias);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeCollection(shards);
        configuration.writeTo(out);
        new PlanStreamOutput(out, configuration).writeNamedWriteableCollection(attributes);
    }

    public String sessionId() {
        return sessionId;
    }

    public String clusterAlias() {
        return clusterAlias;
    }

    public List<ShardDocs> shards() {
        return shards;
    }

    public Configuration configuration() {
        return configuration;
    }

    public List<Attribute> attributes() {
        return attributes;
    }

    public int totalDocs() {
        int docs = 0;
        for (ShardDocs shard : shards) {
            docs += shard.docCount();
        }
        return docs;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // the request's own parent: a gateway node that forwards the request builds a task of it without one to check its type
        if (getParentTask().isSet() == false) {
            assert false : "a fetch request must have a parent task";
            throw new IllegalStateException("a fetch request must have a parent task");
        }
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return FetchRequest.this.getDescription();
            }
        };
    }

    @Override
    public String getDescription() {
        return "fetch [" + sessionId + "] shards=" + shards.size() + ", docs=" + totalDocs();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchRequest that = (FetchRequest) o;
        return sessionId.equals(that.sessionId)
            && clusterAlias.equals(that.clusterAlias)
            && Arrays.equals(indices, that.indices)
            && indicesOptions.equals(that.indicesOptions)
            && shards.equals(that.shards)
            && configuration.equals(that.configuration)
            && attributes.equals(that.attributes)
            && getParentTask().equals(that.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            sessionId,
            clusterAlias,
            Arrays.hashCode(indices),
            indicesOptions,
            shards,
            configuration,
            attributes,
            getParentTask()
        );
    }
}
