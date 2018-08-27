/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.migration;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class IndexUpgradeInfoResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField INDICES = new ParseField("indices");
    private static final ParseField ACTION_REQUIRED = new ParseField("action_required");

    private static final ConstructingObjectParser<IndexUpgradeInfoResponse, String> PARSER =
        new ConstructingObjectParser<>("IndexUpgradeInfoResponse",
            true,
            (a, c) -> {
            @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>)a[0];
                Map<String, UpgradeActionRequired> actionsRequired = map.entrySet().stream()
                    .filter(e -> {
                        if (e.getValue() instanceof Map == false) {
                            return false;
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> value =(Map<String, Object>)e.getValue();
                        return value.containsKey(ACTION_REQUIRED.getPreferredName());
                    })
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> value = (Map<String, Object>) e.getValue();
                            return UpgradeActionRequired.fromString((String)value.get(ACTION_REQUIRED.getPreferredName()));
                        }
                ));
                return new IndexUpgradeInfoResponse(actionsRequired);
            });

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), INDICES);
    }


    private Map<String, UpgradeActionRequired> actions;

    public IndexUpgradeInfoResponse() {

    }

    public IndexUpgradeInfoResponse(Map<String, UpgradeActionRequired> actions) {
        this.actions = actions;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        actions = in.readMap(StreamInput::readString, UpgradeActionRequired::readFromStream);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(actions, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
    }

    public Map<String, UpgradeActionRequired> getActions() {
        return actions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(INDICES.getPreferredName());
            for (Map.Entry<String, UpgradeActionRequired> entry : actions.entrySet()) {
                builder.startObject(entry.getKey());
                {
                    builder.field(ACTION_REQUIRED.getPreferredName(), entry.getValue().toString());
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexUpgradeInfoResponse response = (IndexUpgradeInfoResponse) o;
        return Objects.equals(actions, response.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions);
    }

    public static IndexUpgradeInfoResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
