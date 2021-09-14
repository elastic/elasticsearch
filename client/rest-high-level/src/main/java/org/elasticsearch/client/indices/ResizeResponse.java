/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.ShardsAcknowledgedResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response from a {@link ResizeRequest} call
 */
public class ResizeResponse extends ShardsAcknowledgedResponse {

    private static final ParseField INDEX = new ParseField("index");
    private static final ConstructingObjectParser<ResizeResponse, Void> PARSER = new ConstructingObjectParser<>("resize_index",
        true, args -> new ResizeResponse((boolean) args[0], (boolean) args[1], (String) args[2]));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(AcknowledgedResponse.PARSE_FIELD_NAME));
        PARSER.declareBoolean(constructorArg(), new ParseField(SHARDS_PARSE_FIELD_NAME));
        PARSER.declareField(constructorArg(), (parser, context) -> parser.textOrNull(), INDEX, ObjectParser.ValueType.STRING_OR_NULL);
    }

    private final String index;

    public ResizeResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged);
        this.index = index;
    }

    public String index() {
        return index;
    }

    public static ResizeResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ResizeResponse that = (ResizeResponse) o;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }
}
