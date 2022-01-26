/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response for a create index action.
 */
public class CreateIndexResponse extends ShardsAcknowledgedResponse {

    private static final ParseField INDEX = new ParseField("index");
    private static final ConstructingObjectParser<CreateIndexResponse, Void> PARSER = new ConstructingObjectParser<>(
        "create_index",
        true,
        args -> new CreateIndexResponse((boolean) args[0], (boolean) args[1], (String) args[2])
    );

    static {
        declareAcknowledgedAndShardsAcknowledgedFields(PARSER);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.textOrNull(), INDEX, ObjectParser.ValueType.STRING_OR_NULL);
    }

    private String index;

    public CreateIndexResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged);
        this.index = index;
    }

    public String index() {
        return index;
    }

    public static CreateIndexResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            CreateIndexResponse that = (CreateIndexResponse) o;
            return Objects.equals(index, that.index);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }
}
