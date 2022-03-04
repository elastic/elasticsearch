/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class StatsRequestStats implements Writeable, ToXContentFragment, Iterable<StatsRequestStats.Stats> {

    public static class Stats implements Writeable, ToXContentFragment, Comparable<Stats> {

        private final String request;
        private final long current;
        private final long completed;
        private final long rejected;

        public Stats(String request, long current, long completed, long rejected) {
            this.request = request;
            this.current = current;
            this.completed = completed;
            this.rejected = rejected;
        }

        public Stats(StreamInput in) throws IOException {
            request = in.readString();
            current = in.readLong();
            completed = in.readLong();
            rejected = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(request);
            out.writeLong(current);
            out.writeLong(completed);
            out.writeLong(rejected);
        }

        public String getRequest() {
            return this.request;
        }

        public long getCurrent() {
            return this.current;
        }

        public long getCompleted() {
            return this.completed;
        }

        public long getRejected() {
            return rejected;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(request);
            if (current != -1) {
                builder.field(Fields.CURRENT, current);
            }
            if (completed != -1) {
                builder.field(Fields.COMPLETED, completed);
            }
            if (rejected != -1) {
                builder.field(Fields.REJECTED, rejected);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int compareTo(Stats other) {
            if ((getRequest() == null) && (other.getRequest() == null)) {
                return 0;
            } else if ((getRequest() != null) && (other.getRequest() == null)) {
                return 1;
            } else if (getRequest() == null) {
                return -1;
            } else {
                int compare = getRequest().compareTo(other.getRequest());
                if (compare == 0) {
                    compare = Long.compare(getCompleted(), other.getCompleted());
                }
                return compare;
            }
        }
    }

    private List<Stats> stats;

    public StatsRequestStats(List<Stats> stats) {
        Collections.sort(stats);
        this.stats = stats;
    }

    public StatsRequestStats(StreamInput in) throws IOException {
        stats = in.readList(Stats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(stats);
    }

    @Override
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

    static final class Fields {
        static final String CURRENT = "current";
        static final String COMPLETED = "completed";
        static final String REJECTED = "rejected";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("stats_requests");
        for (Stats stat : stats) {
            stat.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
