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

    public void addChild(InternalProfileResult child) {
        children.add(child);
    }

    public ArrayList<InternalProfileResult> getChildren() {
        return children;
    }

    public void setGlobalTime(long globalTime) {
        this.globalTime = globalTime;
        for (InternalProfileResult child : children) {
            child.setGlobalTime(globalTime);
        }
    }

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

    public static InternalProfileResult readProfileResults(StreamInput in) throws IOException {
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
            InternalProfileResult child = new InternalProfileResult();
            child.readFrom(in);
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
