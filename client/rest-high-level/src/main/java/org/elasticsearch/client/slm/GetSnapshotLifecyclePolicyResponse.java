/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetSnapshotLifecyclePolicyResponse implements ToXContentObject {

    private final Map<String, SnapshotLifecyclePolicyMetadata> policies;

    public GetSnapshotLifecyclePolicyResponse(Map<String, SnapshotLifecyclePolicyMetadata> policies) {
        this.policies = policies;
    }

    public Map<String, SnapshotLifecyclePolicyMetadata> getPolicies() {
        return this.policies;
    }

    public static GetSnapshotLifecyclePolicyResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        parser.nextToken();

        Map<String, SnapshotLifecyclePolicyMetadata> policies = new HashMap<>();
        while (parser.isClosed() == false) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                final String policyId = parser.currentName();
                SnapshotLifecyclePolicyMetadata policyDefinition = SnapshotLifecyclePolicyMetadata.parse(parser, policyId);
                policies.put(policyId, policyDefinition);
            } else {
                parser.nextToken();
            }
        }
        return new GetSnapshotLifecyclePolicyResponse(policies);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GetSnapshotLifecyclePolicyResponse other = (GetSnapshotLifecyclePolicyResponse) o;
        return Objects.equals(this.policies, other.policies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.policies);
    }
}
