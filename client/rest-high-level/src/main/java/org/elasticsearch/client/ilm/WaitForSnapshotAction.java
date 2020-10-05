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

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which waits for snapshot to be taken (by configured SLM policy).
 */
public class WaitForSnapshotAction implements LifecycleAction, ToXContentObject {

    public static final String NAME = "wait_for_snapshot";
    public static final ParseField POLICY_FIELD = new ParseField("policy");

    private static final ConstructingObjectParser<WaitForSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        true, a -> new WaitForSnapshotAction((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_FIELD);
    }

    private final String policy;

    public static WaitForSnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public WaitForSnapshotAction(String policy) {
        if (Strings.hasText(policy) == false) {
            throw new IllegalArgumentException("policy name must be specified");
        }
        this.policy = policy;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY_FIELD.getPreferredName(), policy);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaitForSnapshotAction that = (WaitForSnapshotAction) o;
        return policy.equals(that.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
