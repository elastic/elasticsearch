/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class NodeUsage extends BaseNodeResponse implements ToXContentFragment {

    private final long timestamp;
    private final long sinceTime;
    private final Map<String, Long> restUsage;
    private final Map<String, Object> aggregationUsage;
    private final Map<String, Long> queriesUsage;

    @SuppressWarnings("unchecked")
    public NodeUsage(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readLong();
        sinceTime = in.readLong();
        restUsage = (Map<String, Long>) in.readGenericValue();
        aggregationUsage = (Map<String, Object>) in.readGenericValue();
        if (in.getVersion().onOrAfter(Version.V_8_5_0)) {
            queriesUsage = (Map<String, Long>) in.readGenericValue();
        } else {
            queriesUsage = null;
        }
    }

    /**
     * @param node
     *            the node these statistics were collected from
     * @param timestamp
     *            the timestamp for when these statistics were collected
     * @param sinceTime
     *            the timestamp for when the collection of these statistics
     *            started
     * @param restUsage
     *            a map containing the counts of the number of times each REST
     *            endpoint has been called
     * @param aggregationUsage
     *            a map containing aggregation types along with their usage counts
     * @param queriesUsage
     *            a map containing query types along with their usage counts
     */
    public NodeUsage(
        DiscoveryNode node,
        long timestamp,
        long sinceTime,
        Map<String, Long> restUsage,
        Map<String, Object> aggregationUsage,
        Map<String, Long> queriesUsage
    ) {
        super(node);
        this.timestamp = timestamp;
        this.sinceTime = sinceTime;
        this.restUsage = restUsage;
        this.aggregationUsage = aggregationUsage;
        this.queriesUsage = queriesUsage;
    }

    /**
     * @return the timestamp for when these statistics were collected
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the timestamp for when the collection of these statistics started
     */
    public long getSinceTime() {
        return sinceTime;
    }

    /**
     * @return a map containing the counts of the number of times each REST
     *         endpoint has been called
     */
    public Map<String, Long> getRestUsage() {
        return restUsage;
    }

    /**
     * @return a map containing aggregation types and for each aggregation type how many times it was used
     */
    public Map<String, Object> getAggregationUsage() {
        return aggregationUsage;
    }

    /**
     * @return a map containing query types and for each query type how many times it was used
     */
    public Map<String, Long> getQueriesUsage() {
        return queriesUsage;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("since", sinceTime);
        if (restUsage != null) {
            builder.field("rest_actions");
            builder.map(restUsage);
        }
        if (aggregationUsage != null) {
            builder.field("aggregations");
            builder.map(aggregationUsage);
        }
        if (queriesUsage != null) {
            builder.field("queries");
            builder.map(queriesUsage);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(timestamp);
        out.writeLong(sinceTime);
        out.writeGenericValue(restUsage);
        out.writeGenericValue(aggregationUsage);
        if (out.getVersion().onOrAfter(Version.V_8_5_0)) {
            out.writeGenericValue(queriesUsage);
        }
    }

}
