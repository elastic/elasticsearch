/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.util.Base64;

public record RemoteAccessAuthentication(Authentication authentication, RoleDescriptorsIntersection roleDescriptorsIntersection) {

    public String encode() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(authentication.getEffectiveSubject().getVersion());
        Version.writeVersion(authentication.getEffectiveSubject().getVersion(), output);
        writeTo(output);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
    }

    public void writeTo(StreamOutput out) throws IOException {
        authentication.writeTo(out);
        roleDescriptorsIntersection.writeTo(out);
    }

    public void writeToContext(ThreadContext ctx) throws IOException {
        ctx.putHeader("_remote_access_authentication", encode());
    }
}
