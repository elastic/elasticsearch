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

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class NodeUsage extends BaseNodeResponse implements ToXContentFragment {

    private final long timestamp;
    private final long sinceTime;
    private final Map<String, Long> restUsage;
    private final Map<String, Object> aggregationUsage;

    @SuppressWarnings("unchecked")
    public NodeUsage(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readLong();
        sinceTime = in.readLong();
        restUsage = (Map<String, Long>) in.readGenericValue();
        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            aggregationUsage = (Map<String, Object>) in.readGenericValue();
        } else {
            aggregationUsage = null;
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
     */
    public NodeUsage(DiscoveryNode node, long timestamp, long sinceTime, Map<String, Long> restUsage,
                     Map<String, Object> aggregationUsage) {
        super(node);
        this.timestamp = timestamp;
        this.sinceTime = sinceTime;
        this.restUsage = restUsage;
        this.aggregationUsage = aggregationUsage;
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
     * @return a map containing the counts of the number of times each REST
     *         endpoint has been called
     */
    public Map<String, Object> getAggregationUsage() {
        return aggregationUsage;
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
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(timestamp);
        out.writeLong(sinceTime);
        out.writeGenericValue(restUsage);
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeGenericValue(aggregationUsage);
        }
    }

}
