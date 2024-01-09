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
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

public class GetAllShardSizesAction {

    public static final String NAME = "cluster:monitor/stateless/autoscaling/get_all_shard_sizes";
    public static final ActionType<GetAllShardSizesAction.Response> INSTANCE = ActionType.localOnly(NAME);

    private GetAllShardSizesAction() {/* no instances */}

    public static class TransportGetAllShardSizes extends TransportAction<Request, Response> {

        private final ShardSizeStatsReader reader;

        @Inject
        public TransportGetAllShardSizes(
            ClusterService clusterService,
            IndicesService indicesService,
            ActionFilters actionFilters,
            TransportService transportService
        ) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.reader = new ShardSizeStatsReader(clusterService, indicesService);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            ActionListener.completeWith(listener, () -> new Response(reader.getAllShardSizes(request.interactiveDataAge)));
        }
    }

    public static class Request extends ActionRequest {

        private final TimeValue interactiveDataAge;

        public Request(TimeValue interactiveDataAge) {
            this.interactiveDataAge = interactiveDataAge;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.interactiveDataAge = new TimeValue(in.readLong());
        }

        public TimeValue getInteractiveDataAge() {
            return interactiveDataAge;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(interactiveDataAge.getMillis());
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {

        @Nullable
        private final Map<ShardId, ShardSize> shardSizes;

        public Response(Map<ShardId, ShardSize> shardSizes) {
            this.shardSizes = shardSizes;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.shardSizes = in.readImmutableMap(ShardId::new, ShardSize::from);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(shardSizes);
        }

        public Map<ShardId, ShardSize> getShardSizes() {
            return shardSizes;
        }
    }
}
