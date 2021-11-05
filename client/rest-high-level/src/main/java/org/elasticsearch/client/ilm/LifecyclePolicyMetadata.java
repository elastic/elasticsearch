/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class LifecyclePolicyMetadata implements ToXContentObject {

    static final ParseField POLICY = new ParseField("policy");
    static final ParseField VERSION = new ParseField("version");
    static final ParseField MODIFIED_DATE = new ParseField("modified_date");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<LifecyclePolicyMetadata, String> PARSER = new ConstructingObjectParser<>(
        "policy_metadata",
        true,
        a -> {
            LifecyclePolicy policy = (LifecyclePolicy) a[0];
            return new LifecyclePolicyMetadata(policy, (long) a[1], ZonedDateTime.parse((String) a[2]).toInstant().toEpochMilli());
        }
    );
    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), LifecyclePolicy::parse, POLICY);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODIFIED_DATE);
    }

    public static LifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    private final LifecyclePolicy policy;
    private final long version;
    private final long modifiedDate;

    public LifecyclePolicyMetadata(LifecyclePolicy policy, long version, long modifiedDate) {
        this.policy = policy;
        this.version = version;
        this.modifiedDate = modifiedDate;
    }

    public LifecyclePolicy getPolicy() {
        return policy;
    }

    public String getName() {
        return policy.getName();
    }

    public long getVersion() {
        return version;
    }

    public long getModifiedDate() {
        return modifiedDate;
    }

    public String getModifiedDateString() {
        ZonedDateTime modifiedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(modifiedDate), ZoneOffset.UTC);
        return modifiedDateTime.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(VERSION.getPreferredName(), version);
        builder.field(
            MODIFIED_DATE.getPreferredName(),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(modifiedDate), ZoneOffset.UTC).toString()
        );
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, version, modifiedDate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LifecyclePolicyMetadata other = (LifecyclePolicyMetadata) obj;
        return Objects.equals(policy, other.policy)
            && Objects.equals(version, other.version)
            && Objects.equals(modifiedDate, other.modifiedDate);
    }

}
