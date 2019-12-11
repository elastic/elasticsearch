/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

public class WatcherMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String TYPE = "watcher";

    private final boolean manuallyStopped;

    public WatcherMetaData(boolean manuallyStopped) {
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
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY);
    }

    public WatcherMetaData(StreamInput streamInput) throws IOException {
        this(streamInput.readBoolean());
    }

    public static NamedDiff<MetaData.Custom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(MetaData.Custom.class, TYPE, streamInput);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeBoolean(manuallyStopped);
    }

    @Override
    public String toString() {
        return "manuallyStopped["+ manuallyStopped +"]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatcherMetaData action = (WatcherMetaData) o;

        return manuallyStopped == action.manuallyStopped;
    }

    @Override
    public int hashCode() {
        return Objects.hash(manuallyStopped);
    }

    public static MetaData.Custom fromXContent(XContentParser parser) throws IOException {
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
            return new WatcherMetaData(manuallyStopped);
        }
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Field.MANUALLY_STOPPED.getPreferredName(), manuallyStopped);
        return builder;
    }

    interface Field {

        ParseField MANUALLY_STOPPED = new ParseField("manually_stopped");

    }
}
