/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.segments.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;


public class RestUpgradeAction extends BaseRestHandler {

    @Inject
    public RestUpgradeAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_upgrade", this);
        controller.registerHandler(POST, "/{index}/_upgrade", this);

        controller.registerHandler(GET, "/_upgrade", this);
        controller.registerHandler(GET, "/{index}/_upgrade", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        if (request.method().equals(RestRequest.Method.GET)) {
            handleGet(request, channel, client);
        } else if (request.method().equals(RestRequest.Method.POST)) {
            handlePost(request, channel, client);
        }
    }

    void handleGet(RestRequest request, RestChannel channel, Client client) {
        IndicesSegmentsRequest segsReq = new IndicesSegmentsRequest(Strings.splitStringByCommaToArray(request.param("index")));
        client.admin().indices().segments(segsReq, new RestBuilderListener<IndicesSegmentResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndicesSegmentResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                
                // TODO: getIndices().values() is what IndicesSegmentsResponse uses, but this will produce different orders with jdk8?
                for (IndexSegments indexSegments : response.getIndices().values()) {
                    builder.startObject(indexSegments.getIndex());
                    buildUpgradeStatus(indexSegments, builder);
                    builder.endObject();
                }
                
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
    
    void handlePost(RestRequest request, RestChannel channel, Client client) {
        OptimizeRequest optimizeReq = new OptimizeRequest(Strings.splitStringByCommaToArray(request.param("index")));
        optimizeReq.flush(true);
        optimizeReq.upgrade(true);
        optimizeReq.upgradeOnlyAncientSegments(request.paramAsBoolean("only_ancient_segments", false));
        optimizeReq.maxNumSegments(Integer.MAX_VALUE); // we just want to upgrade the segments, not actually optimize to a single segment
        client.admin().indices().optimize(optimizeReq, new RestBuilderListener<OptimizeResponse>(channel) {
            @Override
            public RestResponse buildResponse(OptimizeResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                buildBroadcastShardsHeader(builder, response);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
    
    void buildUpgradeStatus(IndexSegments indexSegments, XContentBuilder builder) throws IOException {
        long total_bytes = 0;
        long to_upgrade_bytes = 0;
        long to_upgrade_bytes_ancient = 0;
        for (IndexShardSegments shard : indexSegments) {
            for (ShardSegments segs : shard.getShards()) {
                for (Segment seg : segs.getSegments()) {
                    total_bytes += seg.sizeInBytes;
                    if (seg.version.major != Version.CURRENT.luceneVersion.major) {
                        to_upgrade_bytes_ancient += seg.sizeInBytes;
                        to_upgrade_bytes += seg.sizeInBytes;
                    } else if (seg.version.minor != Version.CURRENT.luceneVersion.minor) {
                        // TODO: this comparison is bogus! it would cause us to upgrade even with the same format
                        // instead, we should check if the codec has changed
                        to_upgrade_bytes += seg.sizeInBytes;
                    }
                }
            }
        }

        builder.byteSizeField(SIZE_IN_BYTES, SIZE, total_bytes);
        builder.byteSizeField(SIZE_TO_UPGRADE_IN_BYTES, SIZE_TO_UPGRADE, to_upgrade_bytes);
        builder.byteSizeField(SIZE_TO_UPGRADE_ANCIENT_IN_BYTES, SIZE_TO_UPGRADE_ANCIENT, to_upgrade_bytes_ancient);
    }

    static final XContentBuilderString SIZE = new XContentBuilderString("size");
    static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
    static final XContentBuilderString SIZE_TO_UPGRADE = new XContentBuilderString("size_to_upgrade");
    static final XContentBuilderString SIZE_TO_UPGRADE_ANCIENT = new XContentBuilderString("size_to_upgrade_ancient");
    static final XContentBuilderString SIZE_TO_UPGRADE_IN_BYTES = new XContentBuilderString("size_to_upgrade_in_bytes");
    static final XContentBuilderString SIZE_TO_UPGRADE_ANCIENT_IN_BYTES = new XContentBuilderString("size_to_upgrade_ancient_in_bytes");
}
