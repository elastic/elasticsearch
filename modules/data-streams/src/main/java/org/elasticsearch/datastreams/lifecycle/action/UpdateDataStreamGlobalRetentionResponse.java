/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public final class UpdateDataStreamGlobalRetentionResponse extends ActionResponse implements ChunkedToXContentObject {

    private final boolean acknowledged;
    private final boolean dryRun;
    private final List<AffectedDataStream> affectedDataStreams;

    public UpdateDataStreamGlobalRetentionResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
        dryRun = in.readBoolean();
        affectedDataStreams = in.readCollectionAsImmutableList(AffectedDataStream::read);
    }

    public UpdateDataStreamGlobalRetentionResponse(boolean acknowledged, boolean dryRun) {
        this.acknowledged = acknowledged;
        this.dryRun = dryRun;
        this.affectedDataStreams = List.of();
    }

    public UpdateDataStreamGlobalRetentionResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
        this.dryRun = false;
        this.affectedDataStreams = List.of();
    }

    public UpdateDataStreamGlobalRetentionResponse(boolean acknowledged, List<AffectedDataStream> affectedDataStreams) {
        this.acknowledged = acknowledged;
        this.dryRun = false;
        this.affectedDataStreams = affectedDataStreams;
    }

    public UpdateDataStreamGlobalRetentionResponse(boolean acknowledged, boolean dryRun, List<AffectedDataStream> affectedDataStreams) {
        this.acknowledged = acknowledged;
        this.dryRun = dryRun;
        this.affectedDataStreams = affectedDataStreams;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        out.writeBoolean(dryRun);
        out.writeCollection(affectedDataStreams);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(ChunkedToXContentHelper.startObject(), Iterators.single(((builder, params1) -> {
            builder.field("acknowledged", acknowledged);
            builder.field("dry_run", dryRun);
            return builder;
        })),
            ChunkedToXContentHelper.startArray("affected_data_streams"),
            Iterators.map(affectedDataStreams.iterator(), affectedDataStream -> affectedDataStream::toXContent),
            ChunkedToXContentHelper.endArray(),
            ChunkedToXContentHelper.endObject()
        );
    }

    public record AffectedDataStream(String dataStreamName, TimeValue newEffectiveRetention, TimeValue previousEffectiveRetention)
        implements
            Writeable,
            ToXContentObject {

        public static AffectedDataStream read(StreamInput in) throws IOException {
            return new AffectedDataStream(in.readString(), in.readOptionalTimeValue(), in.readOptionalTimeValue());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(dataStreamName);
            out.writeOptionalTimeValue(newEffectiveRetention);
            out.writeOptionalTimeValue(previousEffectiveRetention);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", dataStreamName);
            builder.field("new_effective_retention", newEffectiveRetention == null ? "infinite" : newEffectiveRetention.getStringRep());
            builder.field(
                "previous_effective_retention",
                previousEffectiveRetention == null ? "infinite" : previousEffectiveRetention.getStringRep()
            );
            builder.endObject();
            return builder;
        }
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public List<AffectedDataStream> getAffectedDataStreams() {
        return affectedDataStreams;
    }
}
