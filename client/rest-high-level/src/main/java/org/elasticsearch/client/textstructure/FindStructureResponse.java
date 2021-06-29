/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.textstructure;

import org.elasticsearch.client.textstructure.structurefinder.TextStructure;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class FindStructureResponse implements ToXContentObject {

    private final TextStructure textStructure;

    FindStructureResponse(TextStructure textStructure) {
        this.textStructure = Objects.requireNonNull(textStructure);
    }

    public static FindStructureResponse fromXContent(XContentParser parser) throws IOException {
        return new FindStructureResponse(TextStructure.PARSER.parse(parser, null).build());
    }

    public TextStructure getFileStructure() {
        return textStructure;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        textStructure.toXContent(builder, params);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(textStructure);
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FindStructureResponse that = (FindStructureResponse) other;
        return Objects.equals(textStructure, that.textStructure);
    }
}
