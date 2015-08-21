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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
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

import static org.elasticsearch.common.lucene.search.NoopCollector.NOOP_COLLECTOR;

/**
 * This class wraps a Lucene Collector and times the execution of:
 * - setScorer()
 * - collect()
 * - doSetNextReader()
 * - needsScores()
 *
 * Because Collectors are (relatively) simple, this class also acts as the repository
 * for timing values and is the class that is serialized between nodes after the search
 * is complete.  When the InternalProfileCollector is serialized, only the timing
 * information is sent (not the wrapped Collector), so it cannot be used for another
 * search
 *
 * InternalProfiler facilitates the linking of the the Collector graph
 */
public class InternalProfileCollector extends SimpleCollector implements CollectorResult, ToXContent, Streamable {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField TIME = new ParseField("time");
    private static final ParseField RELATIVE_TIME = new ParseField("relative_time");
    private static final ParseField CHILDREN = new ParseField("children");

    /**
     * Used to cast a list of InternalProfileCollector into CollectorResult's for
     * external consumption
     */
    private static final Function<InternalProfileCollector, CollectorResult> SUPERTYPE_CAST = new Function<InternalProfileCollector, CollectorResult>() {
        @Override
        public CollectorResult apply(InternalProfileCollector input) {
            return input;
        }
    };

    private Collector collector;
    private LeafCollector leafCollector;

    /**
     * A more friendly representation of the Collector's class name
     */
    private String collectorName;

    /**
     * A "hint" to help provide some context about this Collector
     */
    private CollectorReason reason;

    /**
     * The total elapsed time for this Collector
     */
    private long time;

    /**
     * The total elapsed time for all Collectors across all shards.  This is
     * only populated after the query has finished and timings are finalized
     */
    private long globalTime;

    private List<InternalProfileCollector> children = new ArrayList<>(5);

    public InternalProfileCollector(Collector collector, CollectorReason reason) {
        this.collector = (collector == null) ? NOOP_COLLECTOR : collector;
        this.reason = reason;
        this.collectorName = deriveCollectorName(this.collector);
    }

    /**
     * Creates a human-friendly representation of the Collector name.
     *
     * Bucket Collectors use the aggregation name in their toString() method,
     * which makes the profiled output a bit nicer.
     *
     * @param c The Collector to derive a name from
     * @return  A (hopefully) prettier name
     */
    private String deriveCollectorName(Collector c) {
        String s = c.getClass().getSimpleName();

        // MutiCollector which wraps multiple BucketCollectors is generated
        // via an anonymous class, so this corrects the lack of a name by
        // asking the enclosingClass
        if (s.equals("")) {
            s = c.getClass().getEnclosingClass().getSimpleName();
        }

        // Aggregation collector toString()'s include the user-defined agg name
        if (reason.equals(CollectorReason.AGGREGATION) || reason.equals(CollectorReason.AGGREGATION_GLOBAL)) {
            s += ": [" + c.toString() + "]";
        }
        return s;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        long start = System.nanoTime();
        leafCollector.setScorer(scorer);
        time += System.nanoTime() - start;
    }

    @Override
    public void collect(int doc) throws IOException {
        long start = System.nanoTime();
        leafCollector.collect(doc);
        time += System.nanoTime() - start;
    }

    @Override
    public void doSetNextReader(LeafReaderContext atomicReaderContext) throws IOException {
        long start = System.nanoTime();
        leafCollector = collector.getLeafCollector(atomicReaderContext);
        time += System.nanoTime() - start;
    }

    @Override
    public boolean needsScores() {
        long start = System.nanoTime();
        boolean needsScores = collector.needsScores();
        time += System.nanoTime() - start;
        return needsScores;
    }

    /**
     * Returns the reason "hint"
     */
    @Override
    public CollectorReason getReason() {
        return reason;
    }

    /**
     * Set's the global time across all shards.  Should be called
     * after the entire query (and aggregations!) have finished
     * executing
     *
     * @param globalTime The total time for all Collectors across all Shards
     */
    public void setGlobalCollectorTime(long globalTime) {
        this.globalTime = globalTime;
        for (InternalProfileCollector child : children) {
            child.setGlobalCollectorTime(globalTime);
        }
    }

    /**
     * Returns the elapsed time for this Collector, inclusive of children
     */
    @Override
    public long getTime() {
        if (children.size() == 0) {
            return time;
        }

        long totalTime = time;
        for (InternalProfileCollector child : children) {
            // Global bucket collectors happen after the search, so they won't be
            // included in the time naturally
            if (child.getReason().equals(CollectorReason.AGGREGATION_GLOBAL)) {
                totalTime += child.getTime();
            }
        }
        return totalTime;
    }

    /**
     * Returns the elapsed time relative to the global time
     */
    @Override
    public double getRelativeTime() {
        return getTime() / (double) globalTime;
    }

    /**
     * Add a child "nested" Collector to this Collector.
     * @param child The child Collector to add
     */
    public void addChild(InternalProfileCollector child) {
        if (child == null || children.contains(child)) {
            return;
        }
        children.add(child);
    }

    @Override
    public List<CollectorResult> getProfiledChildren() {
        return Lists.transform(children, SUPERTYPE_CAST);
    }
    @Override
    public String getName() {
        return collectorName;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = builder.startObject()
                .field(NAME.getPreferredName(), toString())
                .field(REASON.getPreferredName(), reason.toString())
                .field(TIME.getPreferredName(), String.format(Locale.US, "%.10gms", (double) (getTime() / 1000000.0)))
                .field(RELATIVE_TIME.getPreferredName(), String.format(Locale.US, "%.10g%%", getRelativeTime() * 100.0))
                .startArray(CHILDREN.getPreferredName());

        for (InternalProfileCollector child : children) {
            builder = child.toXContent(builder, params);
        }
        builder = builder.endArray().endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        collectorName = in.readString();
        reason = CollectorReason.fromInt(in.readVInt());
        time = in.readVLong();
        globalTime = in.readVLong();

        int size = in.readVInt();
        children = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            InternalProfileCollector child = readProfileCollectorFromStream(in);
            children.add(child);
        }
    }

    public static InternalProfileCollector readProfileCollectorFromStream(StreamInput in) throws IOException {
        InternalProfileCollector newInternalProfileCollector = new InternalProfileCollector(null, CollectorReason.GENERAL);
        newInternalProfileCollector.readFrom(in);
        return newInternalProfileCollector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(collectorName);
        out.writeVInt(reason.getReason());
        out.writeVLong(time);
        out.writeVLong(globalTime);
        out.writeVInt(children.size());
        for (InternalProfileCollector child : children) {
            child.writeTo(out);
        }
    }
}
