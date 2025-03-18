/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

public class WatcherMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "watcher";

    private final boolean manuallyStopped;

    public WatcherMetadata(boolean manuallyStopped) {
        this.manuallyStopped = manuallyStopped;
    }

    public boolean manuallyStopped() {
        return manuallyStopped;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY);
    }

    public WatcherMetadata(StreamInput streamInput) throws IOException {
        this(streamInput.readBoolean());
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, streamInput);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeBoolean(manuallyStopped);
    }

    @Override
    public String toString() {
        return "manuallyStopped[" + manuallyStopped + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatcherMetadata action = (WatcherMetadata) o;

        return manuallyStopped == action.manuallyStopped;
    }

    @Override
    public int hashCode() {
        return Objects.hash(manuallyStopped);
    }

    public static Metadata.ProjectCustom fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        Boolean manuallyStopped = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    break;
                case VALUE_BOOLEAN:
                    if (Field.MANUALLY_STOPPED.match(currentFieldName, parser.getDeprecationHandler())) {
                        manuallyStopped = parser.booleanValue();
                    }
                    break;
            }
        }
        if (manuallyStopped != null) {
            return new WatcherMetadata(manuallyStopped);
        }
        return null;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return chunk((b, p) -> b.field(Field.MANUALLY_STOPPED.getPreferredName(), manuallyStopped));
    }

    interface Field {

        ParseField MANUALLY_STOPPED = new ParseField("manually_stopped");

    }
}
