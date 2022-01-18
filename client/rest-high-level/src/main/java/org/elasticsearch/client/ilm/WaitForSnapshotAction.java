/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which waits for snapshot to be taken (by configured SLM policy).
 */
public class WaitForSnapshotAction implements LifecycleAction, ToXContentObject {

    public static final String NAME = "wait_for_snapshot";
    public static final ParseField POLICY_FIELD = new ParseField("policy");

    private static final ConstructingObjectParser<WaitForSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new WaitForSnapshotAction((String) a[0])
    );

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

    public String getPolicy() {
        return policy;
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
