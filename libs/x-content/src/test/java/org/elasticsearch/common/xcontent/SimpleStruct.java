/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Simple structure with 3 fields: int, double and String.
 * Used for testing parsers.
 */
class SimpleStruct implements ToXContentObject {

    static SimpleStruct fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ParseField I = new ParseField("i");
    private static final ParseField D = new ParseField("d");
    private static final ParseField S = new ParseField("s");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SimpleStruct, Void> PARSER =
        new ConstructingObjectParser<>(
            "simple_struct", true, args -> new SimpleStruct((int) args[0], (double) args[1], (String) args[2]));

    static {
        PARSER.declareInt(constructorArg(), I);
        PARSER.declareDouble(constructorArg(), D);
        PARSER.declareString(constructorArg(), S);
    }

    private final int i;
    private final double d;
    private final String s;

    SimpleStruct(int i, double d, String s) {
        this.i = i;
        this.d = d;
        this.s = s;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(I.getPreferredName(), i)
            .field(D.getPreferredName(), d)
            .field(S.getPreferredName(), s)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleStruct other = (SimpleStruct) o;
        return i == other.i && d == other.d && Objects.equals(s, other.s);
    }

    @Override
    public int hashCode() {
        return Objects.hash(i, d, s);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

