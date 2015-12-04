/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;

public class WatcherMetaData extends AbstractDiffable<MetaData.Custom> implements MetaData.Custom {

    public final static String TYPE = "watcher";
    public final static WatcherMetaData PROTO = new WatcherMetaData(false);

    private final boolean manuallyStopped;

    public WatcherMetaData(boolean manuallyStopped) {
        this.manuallyStopped = manuallyStopped;
    }

    public boolean manuallyStopped() {
        return manuallyStopped;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY);
    }

    @Override
    public MetaData.Custom readFrom(StreamInput streamInput) throws IOException {
        return new WatcherMetaData(streamInput.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeBoolean(manuallyStopped);
    }

    @Override
    public MetaData.Custom fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    break;
                case VALUE_BOOLEAN:
                    if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.MANUALLY_STOPPED)) {
                        return new WatcherMetaData(parser.booleanValue());
                    }
                    break;
            }
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
