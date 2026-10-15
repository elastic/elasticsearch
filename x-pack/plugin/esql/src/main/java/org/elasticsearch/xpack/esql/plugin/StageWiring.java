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
    Output output
) implements Writeable {
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
            readOutput(in)
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

    static String exchangeId(String root, String edgeKey, int consumerLane) {
        return root + "/stage/" + edgeKey + "/" + consumerLane;
    }

    private static Output readOutput(StreamInput in) throws IOException {
        return new Output(in);
    }

    /** An incoming edge and the nodes that produce its pages. */
    record Input(String edgeKey, List<String> producerNodeIds) implements Writeable {
        Input {
            producerNodeIds = List.copyOf(producerNodeIds);
        }

        Input(StreamInput in) throws IOException {
            this(in.readString(), in.readCollectionAsList(StreamInput::readString));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(edgeKey);
            out.writeStringCollection(producerNodeIds);
        }
    }

    /** An outgoing edge and the number of consumer lanes it serves. */
    record Output(String edgeKey, ExchangeDistribution distribution, int consumerLanes) implements Writeable {
        Output(StreamInput in) throws IOException {
            this(in.readString(), in.readEnum(ExchangeDistribution.class), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(edgeKey);
            out.writeEnum(distribution);
            out.writeVInt(consumerLanes);
        }
    }
}
