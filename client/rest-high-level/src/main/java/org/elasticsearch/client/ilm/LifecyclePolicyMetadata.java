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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

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
            "policy_metadata", true,
            a -> {
                LifecyclePolicy policy = (LifecyclePolicy) a[0];
                return new LifecyclePolicyMetadata(policy, (long) a[1], ZonedDateTime.parse((String) a[2]).toInstant().toEpochMilli());
            });
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
        builder.field(MODIFIED_DATE.getPreferredName(),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(modifiedDate), ZoneOffset.UTC).toString());
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
        return Objects.equals(policy, other.policy) &&
            Objects.equals(version, other.version) &&
            Objects.equals(modifiedDate, other.modifiedDate);
    }

}
