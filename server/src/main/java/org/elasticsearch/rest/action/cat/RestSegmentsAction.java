/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestSegmentsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/segments"), new Route(GET, "/_cat/segments/{index}"));
    }

    @Override
    public String getName() {
        return "cat_segments_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        clusterStateRequest.clear().nodes(true).routingTable(true).indices(indices);

        final RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());

        return channel -> cancelClient.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                final IndicesSegmentsRequest indicesSegmentsRequest = new IndicesSegmentsRequest();
                indicesSegmentsRequest.indices(indices);
                cancelClient.admin().indices().segments(indicesSegmentsRequest, new RestResponseListener<IndicesSegmentResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(final IndicesSegmentResponse indicesSegmentResponse) throws Exception {
                        if (request.getHttpChannel().isOpen() == false) {
                            throw new TaskCancelledException("response channel [" + request.getHttpChannel() + "] closed");
                        }
                        final Map<String, IndexSegments> indicesSegments = indicesSegmentResponse.getIndices();
                        Table tab = buildTable(request, clusterStateResponse, indicesSegments);
                        return RestTable.buildResponse(tab, channel);
                    }
                });
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/segments\n");
        sb.append("/_cat/segments/{index}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("index", "default:true;alias:i,idx;desc:index name");
        table.addCell("shard", "default:true;alias:s,sh;desc:shard name");
        table.addCell("prirep", "alias:p,pr,primaryOrReplica;default:true;desc:primary or replica");
        table.addCell("ip", "default:true;desc:ip of node where it lives");
        table.addCell("id", "default:false;desc:unique id of node where it lives");
        table.addCell("segment", "default:true;alias:seg;desc:segment name");
        table.addCell("generation", "default:true;alias:g,gen;text-align:right;desc:segment generation");
        table.addCell("docs.count", "default:true;alias:dc,docsCount;text-align:right;desc:number of docs in segment");
        table.addCell("docs.deleted", "default:true;alias:dd,docsDeleted;text-align:right;desc:number of deleted docs in segment");
        table.addCell("size", "default:true;alias:si;text-align:right;desc:segment size in bytes");
        table.addCell("size.memory", "default:true;alias:sm,sizeMemory;text-align:right;desc:segment memory in bytes");
        table.addCell("committed", "default:true;alias:ic,isCommitted;desc:is segment committed");
        table.addCell("searchable", "default:true;alias:is,isSearchable;desc:is segment searched");
        table.addCell("version", "default:true;alias:v,ver;desc:version");
        table.addCell("compound", "default:true;alias:ico,isCompound;desc:is segment compound");
        table.endHeaders();
        return table;
    }

    private Table buildTable(final RestRequest request, ClusterStateResponse state, Map<String, IndexSegments> indicesSegments) {
        Table table = getTableWithHeader(request);

        DiscoveryNodes nodes = state.getState().nodes();

        for (IndexSegments indexSegments : indicesSegments.values()) {
            Map<Integer, IndexShardSegments> shards = indexSegments.getShards();

            for (IndexShardSegments indexShardSegments : shards.values()) {
                ShardSegments[] shardSegments = indexShardSegments.shards();

                for (ShardSegments shardSegment : shardSegments) {
                    List<Segment> segments = shardSegment.getSegments();

                    for (Segment segment : segments) {
                        table.startRow();

                        table.addCell(shardSegment.getShardRouting().getIndexName());
                        table.addCell(shardSegment.getShardRouting().getId());
                        table.addCell(shardSegment.getShardRouting().primary() ? "p" : "r");
                        table.addCell(nodes.get(shardSegment.getShardRouting().currentNodeId()).getHostAddress());
                        table.addCell(shardSegment.getShardRouting().currentNodeId());
                        table.addCell(segment.getName());
                        table.addCell(segment.getGeneration());
                        table.addCell(segment.getNumDocs());
                        table.addCell(segment.getDeletedDocs());
                        table.addCell(segment.getSize());
                        table.addCell(0L);
                        table.addCell(segment.isCommitted());
                        table.addCell(segment.isSearch());
                        table.addCell(segment.getVersion());
                        table.addCell(segment.isCompound());

                        table.endRow();
                    }

                }
            }

        }

        return table;
    }
}
