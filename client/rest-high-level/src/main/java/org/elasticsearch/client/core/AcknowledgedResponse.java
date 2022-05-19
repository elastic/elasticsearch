/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class AcknowledgedResponse {

    protected static final String PARSE_FIELD_NAME = "acknowledged";
    private static final ConstructingObjectParser<AcknowledgedResponse, Void> PARSER = AcknowledgedResponse.generateParser(
        "acknowledged_response",
        AcknowledgedResponse::new,
        AcknowledgedResponse.PARSE_FIELD_NAME
    );

    private final boolean acknowledged;

    public AcknowledgedResponse(final boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    protected static <T> ConstructingObjectParser<T, Void> generateParser(String name, Function<Boolean, T> ctor, String parseField) {
        ConstructingObjectParser<T, Void> p = new ConstructingObjectParser<>(name, true, args -> ctor.apply((boolean) args[0]));
        p.declareBoolean(constructorArg(), new ParseField(parseField));
        return p;
    }

    public static AcknowledgedResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AcknowledgedResponse that = (AcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged);
    }

    /**
     * @return the field name this response uses to output the acknowledged flag
     */
    protected String getFieldName() {
        return PARSE_FIELD_NAME;
    }
}
