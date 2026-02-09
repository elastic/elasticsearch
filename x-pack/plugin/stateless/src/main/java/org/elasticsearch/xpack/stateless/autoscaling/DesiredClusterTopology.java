/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class DesiredClusterTopology implements Writeable {

    private final TierTopology search;
    private final TierTopology index;

    public DesiredClusterTopology(TierTopology search, TierTopology index) {
        this.search = search;
        this.index = index;
    }

    public DesiredClusterTopology(StreamInput in) throws IOException {
        this.search = new TierTopology(in);
        this.index = new TierTopology(in);
    }

    public static DesiredClusterTopology fromXContent(XContentParser parser) {
        return ROOT_PARSER.apply(parser, null);
    }

    public TierTopology getSearch() {
        return search;
    }

    public TierTopology getIndex() {
        return index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        search.writeTo(out);
        index.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        DesiredClusterTopology that = (DesiredClusterTopology) o;
        return Objects.equals(search, that.search) && Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(search, index);
    }

    @Override
    public String toString() {
        return "DesiredClusterTopology{" + "search=" + search + ", index=" + index + '}';
    }

    public static class TierTopology implements Writeable {

        private static final TransportVersion PARSE_DESIRED_TOPOLOGY_TIER_MEMORY = TransportVersion.fromName(
            "parse_desired_topology_tier_memory"
        );

        private final int replicas;
        private final ByteSizeValue memory;
        private final float storageRatio;
        private final float cpuRatio;
        private final float cpuLimitRatio;

        public TierTopology(int replicas, String memory, float storageRatio, float cpuRatio, float cpuLimitRatio) {
            this(
                replicas,
                ByteSizeValue.parseBytesSizeValue(memory, ByteSizeValue.ZERO, "Tier node memory"),
                storageRatio,
                cpuRatio,
                cpuLimitRatio
            );
        }

        public TierTopology(int replicas, ByteSizeValue memory, float storageRatio, float cpuRatio, float cpuLimitRatio) {
            this.replicas = replicas;
            this.memory = memory;
            this.storageRatio = storageRatio;
            this.cpuRatio = cpuRatio;
            this.cpuLimitRatio = cpuLimitRatio;
        }

        TierTopology(StreamInput in) throws IOException {
            this.replicas = in.readInt();
            if (in.getTransportVersion().supports(PARSE_DESIRED_TOPOLOGY_TIER_MEMORY)) {
                this.memory = ByteSizeValue.readFrom(in);
            } else {
                this.memory = ByteSizeValue.parseBytesSizeValue(in.readString(), ByteSizeValue.ZERO, "Tier node memory");
            }
            this.storageRatio = in.readFloat();
            this.cpuRatio = in.readFloat();
            this.cpuLimitRatio = in.readFloat();
        }

        /**
         * The number of nodes in the topology (confusingly called replicas).
         * @return the number of nodes
         */
        public int getReplicas() {
            return replicas;
        }

        public ByteSizeValue getMemory() {
            return memory;
        }

        public float getStorageRatio() {
            return storageRatio;
        }

        public float getCpuRatio() {
            return cpuRatio;
        }

        public float getCpuLimitRatio() {
            return cpuLimitRatio;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(replicas);
            if (out.getTransportVersion().supports(PARSE_DESIRED_TOPOLOGY_TIER_MEMORY)) {
                memory.writeTo(out);
            } else {
                out.writeString(memory.getStringRep());
            }
            out.writeFloat(storageRatio);
            out.writeFloat(cpuRatio);
            out.writeFloat(cpuLimitRatio);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            TierTopology that = (TierTopology) o;
            return replicas == that.replicas
                && Float.compare(storageRatio, that.storageRatio) == 0
                && Float.compare(cpuRatio, that.cpuRatio) == 0
                && Float.compare(cpuLimitRatio, that.cpuLimitRatio) == 0
                && Objects.equals(memory, that.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, memory, storageRatio, cpuRatio, cpuLimitRatio);
        }

        @Override
        public String toString() {
            return "TierTopology{"
                + "replicas="
                + replicas
                + ", memory="
                + memory
                + ", storageRatio="
                + storageRatio
                + ", cpuRatio="
                + cpuRatio
                + ", cpuLimitRatio="
                + cpuLimitRatio
                + '}';
        }
    }

    private static final ConstructingObjectParser<TierTopology, Void> TIER_TOPOLOGY_PARSER = new ConstructingObjectParser<>(
        "tier_topology",
        true,
        args -> new TierTopology((int) args[0], (String) args[1], (float) args[2], (float) args[3], (float) args[4])
    );
    static {
        TIER_TOPOLOGY_PARSER.declareInt(constructorArg(), new ParseField("replicas"));
        TIER_TOPOLOGY_PARSER.declareString(constructorArg(), new ParseField("memory"));
        TIER_TOPOLOGY_PARSER.declareFloat(constructorArg(), new ParseField("storageRatio"));
        TIER_TOPOLOGY_PARSER.declareFloat(constructorArg(), new ParseField("cpuRatio"));
        TIER_TOPOLOGY_PARSER.declareFloat(constructorArg(), new ParseField("cpuLimitRatio"));
    }

    private static final ConstructingObjectParser<DesiredClusterTopology, Void> TIER_PARSER = new ConstructingObjectParser<>(
        "tier",
        true,
        args -> new DesiredClusterTopology((TierTopology) args[0], (TierTopology) args[1])
    );
    static {
        TIER_PARSER.declareObject(constructorArg(), TIER_TOPOLOGY_PARSER, new ParseField("search"));
        TIER_PARSER.declareObject(constructorArg(), TIER_TOPOLOGY_PARSER, new ParseField("index"));
    }

    private static final ConstructingObjectParser<DesiredClusterTopology, Void> DESIRED_PARSER = new ConstructingObjectParser<>(
        "desired",
        true,
        args -> (DesiredClusterTopology) args[0]
    );
    static {
        DESIRED_PARSER.declareObject(constructorArg(), TIER_PARSER, new ParseField("tier"));
    }

    private static final ConstructingObjectParser<DesiredClusterTopology, Void> ROOT_PARSER = new ConstructingObjectParser<>(
        "root",
        true,
        args -> (DesiredClusterTopology) args[0]
    );
    static {
        ROOT_PARSER.declareObject(constructorArg(), DESIRED_PARSER, new ParseField("desired"));
    }
}
