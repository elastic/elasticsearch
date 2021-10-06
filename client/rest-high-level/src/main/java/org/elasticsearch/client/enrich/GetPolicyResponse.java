/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public final class GetPolicyResponse {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetPolicyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_policy_response",
        true,
        args -> new GetPolicyResponse((List<NamedPolicy>) args[0])
    );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<NamedPolicy, Void> CONFIG_PARSER = new ConstructingObjectParser<>(
        "config",
        true,
        args -> (NamedPolicy) args[0]
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            CONFIG_PARSER::apply, new ParseField("policies"));
        CONFIG_PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> NamedPolicy.fromXContent(p), new ParseField("config"));
    }

    private final List<NamedPolicy> policies;

    public static GetPolicyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    public GetPolicyResponse(List<NamedPolicy> policies) {
        this.policies = policies;
    }

    public List<NamedPolicy> getPolicies() {
        return policies;
    }
}
