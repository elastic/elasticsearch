/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */


package org.elasticsearch.client.ilm;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetLifecyclePolicyResponse implements ToXContentObject {

    private final ImmutableOpenMap<String, LifecyclePolicyMetadata> policies;

    public GetLifecyclePolicyResponse(ImmutableOpenMap<String, LifecyclePolicyMetadata> policies) {
        this.policies = policies;
    }

    public ImmutableOpenMap<String, LifecyclePolicyMetadata> getPolicies() {
        return policies;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        for (ObjectObjectCursor<String, LifecyclePolicyMetadata> stringLifecyclePolicyObjectObjectCursor : policies) {
            builder.field(stringLifecyclePolicyObjectObjectCursor.key, stringLifecyclePolicyObjectObjectCursor.value);
        }
        builder.endObject();
        return builder;
    }

    public static GetLifecyclePolicyResponse fromXContent(XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, LifecyclePolicyMetadata> policies = ImmutableOpenMap.builder();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        parser.nextToken();

        while (parser.isClosed() == false) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                String policyName = parser.currentName();
                LifecyclePolicyMetadata policyDefinion = LifecyclePolicyMetadata.parse(parser, policyName);
                policies.put(policyName, policyDefinion);
            } else {
                parser.nextToken();
            }
        }

        return new GetLifecyclePolicyResponse(policies.build());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetLifecyclePolicyResponse that = (GetLifecyclePolicyResponse) o;
        return Objects.equals(getPolicies(), that.getPolicies());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPolicies());
    }
}
