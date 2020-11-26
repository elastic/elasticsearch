/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.core;


import org.elasticsearch.common.ParseField;
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
        if (!(obj instanceof MultiTermVectorsResponse)) return false;
        MultiTermVectorsResponse other = (MultiTermVectorsResponse) obj;
        return Objects.equals(responses, other.responses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(responses);
    }

}
