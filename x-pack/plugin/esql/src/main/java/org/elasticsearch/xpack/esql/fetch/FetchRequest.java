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
import org.elasticsearch.xpack.esql.fetch.lifetime.FetchContextRequest;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;
import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY;

/**
 * Asks one data node for the fetched columns of documents that survived a cut. For each shard it names the reader context
 * the query phase kept open and the documents to load, and it carries the fetch plan that loads them.
 * <p>
 * It carries the index expressions of the query, so it is authorized like the query's other requests to the node, with
 * the same wildcards and aliases. The node checks every shard against the access control of the request, so a shard
 * whose index authorization dropped fails instead of reading through an unfiltered reader.
 */
public final class FetchRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable, FetchContextRequest {
    static final TransportVersion ESQL_FETCH = TransportVersion.fromName("esql_fetch_phase_plan");

    /**
     * The documents to load from one shard, sorted by segment and then by doc, without duplicates. The node returns their
     * rows in this order.
     *
     * @param contextId the reader context the query phase kept open for the shard
     */
    public record ShardDocs(ShardId shardId, ShardSearchContextId contextId, int[] segments, int[] docs) implements Writeable {
        public ShardDocs {
            FetchDocsSourceOperator.ShardDocs.checkSorted(segments, docs);
        }

        /**
         * Reads the documents as runs of one segment each: the segment and the length of every run, then the docs. The
         * first doc of a run is written as it is, and every other doc as its gap to the previous one.
         */
        static ShardDocs readFrom(StreamInput in) throws IOException {
            ShardId shardId = new ShardId(in);
            ShardSearchContextId contextId = new ShardSearchContextId(in);
            int[] runSegments = in.readVIntArray();
            int[] runLengths = in.readVIntArray();
            int[] gaps = in.readVIntArray();
            if (runSegments.length != runLengths.length) {
                throw new IllegalArgumentException("[" + runSegments.length + "] segments for [" + runLengths.length + "] runs");
            }
            int[] segments = new int[gaps.length];
            int[] docs = new int[gaps.length];
            int position = 0;
            for (int r = 0; r < runLengths.length; r++) {
                if (runLengths[r] < 1 || runLengths[r] > gaps.length - position) {
                    throw new IllegalArgumentException(
                        "a run of [" + runLengths[r] + "] at [" + position + "] of [" + gaps.length + "] docs"
                    );
                }
                int doc = 0;
                for (int i = 0; i < runLengths[r]; i++) {
                    doc = i == 0 ? gaps[position] : doc + gaps[position];
                    segments[position] = runSegments[r];
                    docs[position] = doc;
                    position++;
                }
            }
            if (position != gaps.length) {
                throw new IllegalArgumentException("the runs hold [" + position + "] of [" + gaps.length + "] docs");
            }
            return new ShardDocs(shardId, contextId, segments, docs);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            contextId.writeTo(out);
            int runs = runs();
            int[] runSegments = new int[runs];
            int[] runLengths = new int[runs];
            int[] gaps = new int[docs.length];
            int run = -1;
            for (int i = 0; i < docs.length; i++) {
                if (i == 0 || segments[i] != segments[i - 1]) {
                    run++;
                    runSegments[run] = segments[i];
                    gaps[i] = docs[i];
                } else {
                    // positive, because the docs of a segment are strictly increasing
                    gaps[i] = docs[i] - docs[i - 1];
                }
                runLengths[run]++;
            }
            out.writeVIntArray(runSegments);
            out.writeVIntArray(runLengths);
            out.writeVIntArray(gaps);
        }

        private int runs() {
            int runs = 0;
            for (int i = 0; i < segments.length; i++) {
                if (i == 0 || segments[i] != segments[i - 1]) {
                    runs++;
                }
            }
            return runs;
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
                && Arrays.equals(segments, that.segments)
                && Arrays.equals(docs, that.docs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, contextId, Arrays.hashCode(segments), Arrays.hashCode(docs));
        }

        @Override
        public String toString() {
            return "ShardDocs[" + shardId + ", " + contextId + ", docs=" + docs.length + "]";
        }
    }

    private final String sessionId;
    private final String clusterAlias;
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private final List<ShardDocs> shards;
    private final Configuration configuration;
    private final PhysicalPlan fetchPlan;
    private List<ShardSearchContextId> releaseAfter;

    /**
     * @param originalIndices the index expressions of the query for the cluster of the node
     * @param fetchPlan       the plan that loads the fetched columns, its output is the schema of the response pages
     * @param releaseAfter    the reader contexts the node frees once it answered, whether the fetch succeeded or not. It
     *                        may name contexts this request doesn't fetch from.
     */
    public FetchRequest(
        String sessionId,
        String clusterAlias,
        OriginalIndices originalIndices,
        List<ShardDocs> shards,
        Configuration configuration,
        PhysicalPlan fetchPlan,
        List<ShardSearchContextId> releaseAfter
    ) {
        this.sessionId = sessionId;
        this.clusterAlias = clusterAlias;
        this.indices = originalIndices.indices();
        this.indicesOptions = originalIndices.indicesOptions();
        this.shards = List.copyOf(shards);
        // the fetch plan reads no lookup tables, so the request doesn't ship them
        this.configuration = configuration.withoutTables();
        this.fetchPlan = fetchPlan;
        this.releaseAfter = List.copyOf(releaseAfter);
    }

    public FetchRequest(StreamInput in) throws IOException {
        this(in, null);
    }

    /**
     * @param idMapper {@code null} in production. Tests map the ids of the plan's attributes so a copy equals the original.
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
        this.fetchPlan = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration, idMapper).readNamedWriteable(
            PhysicalPlan.class
        );
        this.releaseAfter = in.readCollectionAsImmutableList(ShardSearchContextId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_FETCH) == false) {
            // only nodes that ran the query phase with document references get fetch requests
            throw new IllegalStateException("can't send a fetch request to a node on [" + out.getTransportVersion() + "]");
        }
        super.writeTo(out);
        out.writeString(sessionId);
        out.writeString(clusterAlias);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeCollection(shards);
        configuration.writeTo(out);
        new PlanStreamOutput(out, configuration).writeNamedWriteable(fetchPlan);
        out.writeCollection(releaseAfter);
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

    public PhysicalPlan fetchPlan() {
        return fetchPlan;
    }

    public List<ShardSearchContextId> releaseAfter() {
        return releaseAfter;
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

    /**
     * Keeps the shards even when authorization leaves no index: the node then fails each of them, because no index of
     * theirs is in the access control of the request. The request frees none of the contexts it names then, like a free
     * request without an index, and they stay open until their keep-alive passes.
     */
    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        if (Arrays.equals(NO_INDICES_OR_ALIASES_ARRAY, indices) || Arrays.asList(indices).contains(NO_INDEX_PLACEHOLDER)) {
            this.releaseAfter = List.of();
        }
        return this;
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
        if (parentTaskId.isSet() == false) {
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
            && fetchPlan.equals(that.fetchPlan)
            && releaseAfter.equals(that.releaseAfter)
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
            fetchPlan,
            releaseAfter,
            getParentTask()
        );
    }
}
