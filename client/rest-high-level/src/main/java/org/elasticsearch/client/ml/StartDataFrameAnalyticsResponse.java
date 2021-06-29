/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response indicating if the Machine Learning Datafeed is now started or not
 */
public class StartDataFrameAnalyticsResponse extends AcknowledgedResponse {

    private static final ParseField NODE = new ParseField("node");

    public static final ConstructingObjectParser<StartDataFrameAnalyticsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "start_data_frame_analytics_response",
            true,
            (a) -> new StartDataFrameAnalyticsResponse((Boolean) a[0], (String) a[1]));

    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NODE);
    }

    private final String node;

    public StartDataFrameAnalyticsResponse(boolean acknowledged, String node) {
        super(acknowledged);
        this.node = node;
    }

    public static StartDataFrameAnalyticsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * The node that the job was assigned to
     *
     * @return The ID of a node if the job was assigned to a node.  If an empty string is returned
     *         it means the job was allowed to open lazily and has not yet been assigned to a node.
     *         If <code>null</code> is returned it means the server version is too old to return node
     *         information.
     */
    public String getNode() {
        return node;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        StartDataFrameAnalyticsResponse that = (StartDataFrameAnalyticsResponse) other;
        return isAcknowledged() == that.isAcknowledged()
            && Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged(), node);
    }

    @Override
    public void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        if (node != null) {
            builder.field(NODE.getPreferredName(), node);
        }
    }
}
