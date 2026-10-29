/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;

import java.io.IOException;
import java.util.List;

/** Serializable scheduling metadata that lets a data-node request execute one stage. */
record StageWiring(
    String rootSessionId,
    int stageId,
    StagedPlanner.StagePlacement placement,
    List<Integer> localLanes,
    List<Input> inputs,
    Output output,
    boolean stageQualifiedExchangeIds
) implements Writeable {
    private static final int LEGACY_PRODUCER_STAGE_ID = 0;

    StageWiring(
        String rootSessionId,
        int stageId,
        StagedPlanner.StagePlacement placement,
        List<Integer> localLanes,
        List<Input> inputs,
        Output output
    ) {
        this(rootSessionId, stageId, placement, localLanes, inputs, output, true);
    }

    StageWiring {
        localLanes = List.copyOf(localLanes);
        inputs = List.copyOf(inputs);
    }

    StageWiring(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readVInt(),
            in.readEnum(StagedPlanner.StagePlacement.class),
            in.readCollectionAsList(StreamInput::readVInt),
            in.readCollectionAsList(Input::new),
            readOutput(in),
            in.getTransportVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rootSessionId);
        out.writeVInt(stageId);
        out.writeEnum(placement);
        out.writeCollection(localLanes, StreamOutput::writeVInt);
        out.writeCollection(inputs);
        out.writeWriteable(output);
    }

    static String exchangeId(String root, String edgeKey, int producerStage, int consumerLane) {
        return exchangeId(root, edgeKey, producerStage, consumerLane, true);
    }

    static String exchangeId(String root, String edgeKey, int producerStage, int consumerLane, boolean stageQualified) {
        return stageQualified
            ? root + "/stage/" + edgeKey + "/" + producerStage + "/" + consumerLane
            : root + "/stage/" + edgeKey + "/" + consumerLane;
    }

    private static Output readOutput(StreamInput in) throws IOException {
        return new Output(in);
    }

    /** An incoming edge and the nodes that produce its pages. */
    record Input(String edgeKey, List<Producer> producers) implements Writeable {
        Input {
            producers = List.copyOf(producers);
        }

        Input(StreamInput in) throws IOException {
            this(in.readString(), readProducers(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(edgeKey);
            if (out.getTransportVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION)) {
                out.writeCollection(producers);
            } else {
                if (producers.size() != 1) {
                    throw new IllegalStateException("multi-producer shuffle stages require a newer transport version");
                }
                out.writeStringCollection(producers.getFirst().nodeIds());
            }
        }

        private static List<Producer> readProducers(StreamInput in) throws IOException {
            if (in.getTransportVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION)) {
                return in.readCollectionAsList(Producer::new);
            }
            return List.of(new Producer(LEGACY_PRODUCER_STAGE_ID, in.readCollectionAsList(StreamInput::readString)));
        }
    }

    /** A producer stage and the nodes that execute its lanes. */
    record Producer(int stageId, List<String> nodeIds) implements Writeable {
        Producer {
            nodeIds = List.copyOf(nodeIds);
        }

        Producer(StreamInput in) throws IOException {
            this(in.readVInt(), in.readCollectionAsList(StreamInput::readString));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(stageId);
            out.writeStringCollection(nodeIds);
        }
    }

    /** An outgoing edge and the number of consumer lanes it serves. */
    record Output(String edgeKey, ExchangeDistribution distribution, int consumerLanes) implements Writeable {
        Output(StreamInput in) throws IOException {
            this(in.readString(), in.readEnum(ExchangeDistribution.class), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (distribution == ExchangeDistribution.BROADCAST
                && out.getTransportVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION) == false) {
                throw new IllegalStateException(
                    "broadcast shuffle edges require transport version [" + ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION + "]"
                );
            }
            out.writeString(edgeKey);
            out.writeEnum(distribution);
            out.writeVInt(consumerLanes);
        }
    }
}
