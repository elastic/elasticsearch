/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.map;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class MapConfig implements Writeable, ToXContentObject {

    private static final String NAME = "transform_map";

    private static final ConstructingObjectParser<MapConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<MapConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<MapConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<MapConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient, args -> {

            return new MapConfig();
        });

        return parser;
    }

    public MapConfig() {}

    public MapConfig(StreamInput in) throws IOException {}

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    public static MapConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

}
