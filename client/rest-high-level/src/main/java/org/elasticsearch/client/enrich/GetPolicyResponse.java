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
package org.elasticsearch.client.enrich;

import org.elasticsearch.common.ParseField;
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
