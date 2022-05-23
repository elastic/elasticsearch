/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.security.authc.RealmConfig.REALM_IDENTIFIER_PARSER;

public record RealmDomain(String name, Set<RealmConfig.RealmIdentifier> realms) implements Writeable, ToXContentObject {

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(realms);
    }

    static RealmDomain readFrom(StreamInput in) throws IOException {
        String domainName = in.readString();
        Set<RealmConfig.RealmIdentifier> realms = in.readSet(RealmConfig.RealmIdentifier::new);
        return new RealmDomain(domainName, realms);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("name", name);
            // Sort to have stable order in display
            builder.xContentList("realms", realms.stream().sorted().toList());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "RealmDomain{" + "name='" + name + '\'' + ", realms=" + realms + '}';
    }

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<RealmDomain, Void> REALM_DOMAIN_PARSER = new ConstructingObjectParser<>(
        "realm_domain",
        false,
        (args, v) -> new RealmDomain((String) args[0], Set.copyOf((List<RealmConfig.RealmIdentifier>) args[1]))
    );

    static {
        REALM_DOMAIN_PARSER.declareString(constructorArg(), new ParseField("name"));
        REALM_DOMAIN_PARSER.declareObjectArray(constructorArg(), (p, c) -> REALM_IDENTIFIER_PARSER.parse(p, c), new ParseField("realms"));
    }
}
