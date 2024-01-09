/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.lucene.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class GetShardSizeAction {

    public static final String NAME = "cluster:monitor/stateless/autoscaling/get_shard_size";
    public static final ActionType<Response> INSTANCE = ActionType.localOnly(NAME);

    private GetShardSizeAction() {/* no instances */}

    public static class TransportGetShardSize extends TransportAction<Request, Response> {

        private final ShardSizeStatsReader reader;
        private final ThreadPool threadPool;

        @Inject
        public TransportGetShardSize(
            ClusterService clusterService,
            IndicesService indicesService,
            ActionFilters actionFilters,
            TransportService transportService
        ) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.reader = new ShardSizeStatsReader(clusterService, indicesService);
            this.threadPool = clusterService.threadPool();
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            // fork to generic thread pool because computing the shard size might access files on disk and trigger cache misses
            // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
            var run = ActionRunnable.supply(listener, () -> new Response(reader.getShardSize(request.shardId, request.interactiveDataAge)));
            threadPool.generic().execute(run);
        }
    }

    public static class Request extends ActionRequest {

        private final ShardId shardId;
        private final TimeValue interactiveDataAge;

        public Request(ShardId shardId, TimeValue interactiveDataAge) {
            this.shardId = shardId;
            this.interactiveDataAge = interactiveDataAge;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.interactiveDataAge = new TimeValue(in.readLong());
        }

        public ShardId getShardId() {
            return shardId;
        }

        public TimeValue getInteractiveDataAge() {
            return interactiveDataAge;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeWriteable(shardId);
            out.writeLong(interactiveDataAge.getMillis());
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {

        @Nullable
        private final ShardSize shardSize;

        public Response(ShardSize shardSize) {
            this.shardSize = shardSize;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.shardSize = in.readOptionalWriteable(ShardSize::from);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(shardSize);
        }

        public ShardSize getShardSize() {
            return shardSize;
        }
    }
}
