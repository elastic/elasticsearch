/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.xpack;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class ClientEnrollmentRequest implements Validatable, ToXContentObject {

    private final String clientType;
    private final char[] clientPassword;


    public ClientEnrollmentRequest(String clientType, @Nullable char[] clientPassword) {
        this.clientType = Objects.requireNonNull(clientType, "client_type is required");
        this.clientPassword = clientPassword;
    }

    public String getClientType() {
        return clientType;
    }

    public char[] getClientPassword() {
        return clientPassword;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("client_type", clientType);
        if ( clientPassword != null){
            byte[] clientPasswordBytes = CharArrays.toUtf8Bytes(clientPassword);
            try {
                builder.field("client_password").utf8Value(clientPasswordBytes, 0, clientPasswordBytes.length);
            } finally {
                Arrays.fill(clientPasswordBytes, (byte) 0);
            }
        }
        return builder.endObject();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientEnrollmentRequest that = (ClientEnrollmentRequest) o;
        return clientType.equals(that.clientType) && Arrays.equals(clientPassword, that.clientPassword);
    }

    @Override public int hashCode() {
        int result = Objects.hash(clientType);
        result = 31 * result + Arrays.hashCode(clientPassword);
        return result;
    }
}
