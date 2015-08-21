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


import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
    private InternalProfileBreakdown timings;
    private long nodeTime = -1;     // Use -1 instead of Null so it can be serialized, and there should never be a negative time
    private long globalTime;
    private ArrayList<InternalProfileResult> children;

    /**
     * A top level InternalProfileResult will have a Collector.
     * Since a query only has one Collector tree, non-top-level InternalProfileResult's
     * will have a null collector.
     */
    private InternalProfileCollector collector;

    private static final Function<InternalProfileResult, ProfileResult> SUPERTYPE_CAST = new Function<InternalProfileResult, ProfileResult>() {
        @Override
        public ProfileResult apply(InternalProfileResult input) {
            return input;
        }
    };

    public InternalProfileResult(Query query, InternalProfileBreakdown timings) {
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
     * Returns the total time spent at this node inclusive of children times
     * @return Total node time
     */
    public long calculateNodeTime() {
        if (nodeTime != -1) {
            return nodeTime;
        }

        // Collect our local timings
        nodeTime = timings.getTotalTime();

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
    public ProfileBreakdown getTimeBreakdown() {
        return timings;
    }

    @Override
    public long getTime() {
        return nodeTime;
    }

    public void setCollector(InternalProfileCollector collector) {
        this.collector = collector;
    }

    public InternalProfileCollector getCollector() {
        return this.collector;
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
        return Lists.transform(children, SUPERTYPE_CAST);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        queryType = in.readString();
        luceneDescription = in.readString();
        timings = new InternalProfileBreakdown();
        nodeTime = in.readLong();
        globalTime = in.readVLong();
        timings.readFrom(in);
        int size = in.readVInt();
        children = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            children.add(InternalProfileResult.readProfileResult(in));
        }

        if (in.readBoolean()) {
            collector = InternalProfileCollector.readProfileCollectorFromStream(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryType);
        out.writeString(luceneDescription);
        out.writeLong(nodeTime);            // not Vlong because can be negative
        out.writeVLong(globalTime);
        timings.writeTo(out);
        out.writeVInt(children.size());
        for (InternalProfileResult child : children) {
            child.writeTo(out);
        }

        if (collector != null) {
            out.writeBoolean(true);
            collector.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = builder.startObject()
                .field(QUERY_TYPE.getPreferredName(), queryType)
                .field(LUCENE_DESCRIPTION.getPreferredName(), luceneDescription)
                .field(NODE_TIME.getPreferredName(), String.format(Locale.US, "%.10gms", (double)(nodeTime / 1000000.0)))
                .field(RELATIVE_TIME.getPreferredName(), String.format(Locale.US, "%.10g%%", getRelativeTime() * 100.0))
                .startObject(BREAKDOWN.getPreferredName());

        builder = timings.toXContent(builder, params)
                .endObject()
                .startArray(CHILDREN.getPreferredName());

        for (InternalProfileResult child : children) {
            builder = child.toXContent(builder, params);
        }

        builder = builder.endArray().endObject();
        return builder;
    }
}
