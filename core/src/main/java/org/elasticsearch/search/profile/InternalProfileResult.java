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

package org.elasticsearch.search.profile;


import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class is the internal representation of a profiled query, corresponding
 * to a single node in the query tree.  It is built after the query has finished executing
 * and is merely a structured representation, rather than the entity that collects the timing
 * profile (see InternalProfiler for that)
 *
 * Each InternalProfileResult has a List of InternalProfileResults, which will contain
 * "children" queries if applicable
 */
public class InternalProfileResult implements ProfileResult, Streamable, ToXContent {

    private static final ParseField QUERY_TYPE = new ParseField("query_type");
    private static final ParseField LUCENE_DESCRIPTION = new ParseField("lucene");
    private static final ParseField NODE_TIME = new ParseField("time");
    private static final ParseField RELATIVE_TIME = new ParseField("relative_time");
    private static final ParseField CHILDREN = new ParseField("children");
    private static final ParseField BREAKDOWN = new ParseField("breakdown");

    private String queryType;
    private String luceneDescription;
    private Map<String, Long> timings;
    private long nodeTime = -1;     // Use -1 instead of Null so it can be serialized, and there should never be a negative time
    private long globalTime;
    private ArrayList<InternalProfileResult> children;

    public InternalProfileResult(Query query, Map<String, Long> timings) {
        children = new ArrayList<>(5);
        this.queryType = query.getClass().getSimpleName();
        this.luceneDescription = query.toString();
        this.timings = timings;
    }

    public InternalProfileResult() {

    }

    /**
     * Add a child profile result to this node
     * @param child The child to add
     */
    public void addChild(InternalProfileResult child) {
        children.add(child);
    }

    /**
     * Retrieve a list of all profiled children at this node
     * @return List of profiled children
     */
    public ArrayList<InternalProfileResult> getChildren() {
        return children;
    }

    /**
     * Sets the global time for the entire query (across all shards).  This is
     * set retroactively on the coordinating node after all times have been aggregated,
     * and is used to calculate the global relative time.
     *
     * Internally this calls setGlobalTime() recursively on all children to spread
     * the time through the entire tre
     *
     * @param globalTime The global time to execute across all shards
     */
    public void setGlobalTime(long globalTime) {
        this.globalTime = globalTime;
        for (InternalProfileResult child : children) {
            child.setGlobalTime(globalTime);
        }
    }

    /**
     * Overwrite the current timings with a new set of timings
     * @param timings The new set of timings to use
     */
    public void setTimings(Map<String, Long> timings) {
        this.timings = timings;
    }

    /**
     * Returns the total time spent at this node inclusive of children times
     * @return Total node time
     */
    public long calculateNodeTime() {
        if (nodeTime != -1) {
            return nodeTime;
        }

        long nodeTime = 0;
        for (long time : timings.values()) {
            nodeTime += time;
        }
        // Collect our local timings
        this.nodeTime = nodeTime;

        // Then add up our children
        for (InternalProfileResult child : children) {
            child.calculateNodeTime();
        }

        return nodeTime;
    }

    @Override
    public double getRelativeTime() {
        return calculateNodeTime() / (double) globalTime;
    }

    @Override
    public String getLuceneDescription() {
        return luceneDescription;
    }

    @Override
    public String getQueryName() {
        return queryType;
    }

    @Override
    public Map<String, Long> getTimeBreakdown() {
        return timings;
    }

    @Override
    public long getTime() {
        return nodeTime;
    }

    /**
     * Static helper to read an InternalProfileResult off the stream
     */
    public static InternalProfileResult readProfileResult(StreamInput in) throws IOException {
        InternalProfileResult newResults = new InternalProfileResult();
        newResults.readFrom(in);
        return newResults;
    }

    @Override
    public List<ProfileResult> getProfiledChildren() {
        return Collections.unmodifiableList(children);
    }

    private Map<String, Long> readTimings(StreamInput in) throws IOException {
        final int size = in.readVInt();
        Map<String, Long> map = new HashMap<>(size);
        for (int i = 0; i < size; ++i) {
            map.put(in.readString(), in.readLong());
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        queryType = in.readString();
        luceneDescription = in.readString();
        nodeTime = in.readLong();
        globalTime = in.readVLong();
        timings = readTimings(in);
        int size = in.readVInt();
        children = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            children.add(InternalProfileResult.readProfileResult(in));
        }
    }

    private void writeTimings(Map<String, Long> timings, StreamOutput out) throws IOException {
        out.writeVInt(timings.size());
        for (Map.Entry<String, Long> entry : timings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryType);
        out.writeString(luceneDescription);
        out.writeLong(nodeTime);            // not Vlong because can be negative
        out.writeVLong(globalTime);
        writeTimings(timings, out);
        out.writeVInt(children.size());
        for (InternalProfileResult child : children) {
            child.writeTo(out);
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = builder.startObject()
                .field(QUERY_TYPE.getPreferredName(), queryType)
                .field(LUCENE_DESCRIPTION.getPreferredName(), luceneDescription)
                .field(NODE_TIME.getPreferredName(), String.format(Locale.US, "%.10gms", (double)(nodeTime / 1000000.0)))
                .field(RELATIVE_TIME.getPreferredName(), String.format(Locale.US, "%.10g%%", getRelativeTime() * 100.0))
                .field(BREAKDOWN.getPreferredName(), timings)
                .startArray(CHILDREN.getPreferredName());

        for (InternalProfileResult child : children) {
            builder = child.toXContent(builder, params);
        }

        builder = builder.endArray().endObject();
        return builder;
    }
}
