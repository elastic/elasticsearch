/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;


import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class MultiTermVectorsResponse {
    private final List<TermVectorsResponse> responses;

    public MultiTermVectorsResponse(List<TermVectorsResponse> responses) {
        this.responses = responses;
    }

    private static final ConstructingObjectParser<MultiTermVectorsResponse, Void> PARSER =
        new ConstructingObjectParser<>("multi_term_vectors", true,
        args -> {
            // as the response comes from server, we are sure that args[0] will be a list of TermVectorsResponse
            @SuppressWarnings("unchecked") List<TermVectorsResponse> termVectorsResponsesList = (List<TermVectorsResponse>) args[0];
            return new MultiTermVectorsResponse(termVectorsResponsesList);
        }
    );

    static {
        PARSER.declareObjectArray(constructorArg(), (p,c) -> TermVectorsResponse.fromXContent(p), new ParseField("docs"));
    }

    public static MultiTermVectorsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Returns the list of {@code TermVectorsResponse} for this {@code MultiTermVectorsResponse}
     */
    public List<TermVectorsResponse> getTermVectorsResponses() {
        return responses;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if ((obj instanceof MultiTermVectorsResponse) == false) return false;
        MultiTermVectorsResponse other = (MultiTermVectorsResponse) obj;
        return Objects.equals(responses, other.responses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(responses);
    }

}
