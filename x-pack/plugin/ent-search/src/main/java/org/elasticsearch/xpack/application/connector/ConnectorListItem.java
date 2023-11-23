/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This class is used for returning information for lists of connectors, to avoid including all
 * {@link Connector} information which can be retrieved using subsequent GetConnector requests.
 */
public class ConnectorListItem implements Writeable, ToXContentObject {

    private static final ParseField CONNECTOR_ID_FIELD = new ParseField("connector_id");
    private static final ParseField NAME_FIELD = new ParseField("name");

    private final String connectorId;

    @Nullable
    private final String name;

    public ConnectorListItem(String connectorId, @Nullable String name) {
        this.connectorId = connectorId;
        this.name = name;
    }

    public ConnectorListItem(StreamInput in) throws IOException {
        this.connectorId = in.readString();
        this.name = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(CONNECTOR_ID_FIELD.getPreferredName(), connectorId);
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(connectorId);
        out.writeOptionalString(name);
    }
}
