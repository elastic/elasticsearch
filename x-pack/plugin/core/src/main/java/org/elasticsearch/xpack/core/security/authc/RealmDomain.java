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

import java.io.IOException;
import java.util.Set;

public record RealmDomain(String name, Set<RealmConfig.RealmIdentifier> realms) implements Writeable {

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
}
