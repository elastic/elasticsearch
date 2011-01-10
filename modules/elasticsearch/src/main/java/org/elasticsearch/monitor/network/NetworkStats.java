/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.monitor.network;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (shay.banon)
 */
public class NetworkStats implements Streamable, Serializable, ToXContent {

    long timestamp;

    Tcp tcp = null;

    NetworkStats() {

    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("network");
        if (tcp != null) {
            builder.startObject("tcp");
            builder.field("active_opens", tcp.getActiveOpens());
            builder.field("passive_opens", tcp.getPassiveOpens());
            builder.field("curr_estab", tcp.getCurrEstab());
            builder.field("in_segs", tcp.getInSegs());
            builder.field("out_segs", tcp.getOutSegs());
            builder.field("retrans_segs", tcp.getRetransSegs());
            builder.field("estab_resets", tcp.getEstabResets());
            builder.field("attempt_fails", tcp.getAttemptFails());
            builder.field("in_errs", tcp.getInErrs());
            builder.field("out_rsts", tcp.getOutRsts());
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static NetworkStats readNetworkStats(StreamInput in) throws IOException {
        NetworkStats stats = new NetworkStats();
        stats.readFrom(in);
        return stats;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        if (in.readBoolean()) {
            tcp = Tcp.readNetworkTcp(in);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        if (tcp == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            tcp.writeTo(out);
        }
    }

    public long timestamp() {
        return timestamp;
    }

    public long getTimestamp() {
        return timestamp();
    }

    public Tcp tcp() {
        return tcp;
    }

    public Tcp getTcp() {
        return tcp();
    }

    public static class Tcp implements Serializable, Streamable {

        long activeOpens;
        long passiveOpens;
        long attemptFails;
        long estabResets;
        long currEstab;
        long inSegs;
        long outSegs;
        long retransSegs;
        long inErrs;
        long outRsts;

        public static Tcp readNetworkTcp(StreamInput in) throws IOException {
            Tcp tcp = new Tcp();
            tcp.readFrom(in);
            return tcp;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            activeOpens = in.readLong();
            passiveOpens = in.readLong();
            attemptFails = in.readLong();
            estabResets = in.readLong();
            currEstab = in.readLong();
            inSegs = in.readLong();
            outSegs = in.readLong();
            retransSegs = in.readLong();
            inErrs = in.readLong();
            outRsts = in.readLong();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(activeOpens);
            out.writeLong(passiveOpens);
            out.writeLong(attemptFails);
            out.writeLong(estabResets);
            out.writeLong(currEstab);
            out.writeLong(inSegs);
            out.writeLong(outSegs);
            out.writeLong(retransSegs);
            out.writeLong(inErrs);
            out.writeLong(outRsts);
        }

        public long activeOpens() {
            return this.activeOpens;
        }

        public long getActiveOpens() {
            return activeOpens();
        }

        public long passiveOpens() {
            return passiveOpens;
        }

        public long getPassiveOpens() {
            return passiveOpens();
        }

        public long attemptFails() {
            return attemptFails;
        }

        public long getAttemptFails() {
            return attemptFails();
        }

        public long estabResets() {
            return estabResets;
        }

        public long getEstabResets() {
            return estabResets();
        }

        public long currEstab() {
            return currEstab;
        }

        public long getCurrEstab() {
            return currEstab();
        }

        public long inSegs() {
            return inSegs;
        }

        public long getInSegs() {
            return inSegs();
        }

        public long outSegs() {
            return outSegs;
        }

        public long getOutSegs() {
            return outSegs();
        }

        public long retransSegs() {
            return retransSegs;
        }

        public long getRetransSegs() {
            return retransSegs();
        }

        public long inErrs() {
            return inErrs;
        }

        public long getInErrs() {
            return inErrs();
        }

        public long outRsts() {
            return outRsts;
        }

        public long getOutRsts() {
            return outRsts();
        }
    }
}
