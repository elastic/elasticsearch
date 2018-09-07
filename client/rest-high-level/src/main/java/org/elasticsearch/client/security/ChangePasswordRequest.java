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

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public final class ChangePasswordRequest implements Validatable, Closeable, ToXContentObject {

    private final String username;
    private final char[] password;
    private final RefreshPolicy refreshPolicy;

    public ChangePasswordRequest(String username, char[] password, RefreshPolicy refreshPolicy) {
        this.username = username;
        this.password = Objects.requireNonNull(password, "password is required");
        this.refreshPolicy = refreshPolicy == null ? RefreshPolicy.getDefault() : refreshPolicy;
    }


    public String getUsername() {
        return username;
    }

    public char[] getPassword() {
        return password;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChangePasswordRequest that = (ChangePasswordRequest) o;
        return Objects.equals(username, that.username) &&
            Arrays.equals(password, that.password) &&
            refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(username, refreshPolicy);
        result = 31 * result + Arrays.hashCode(password);
        return result;
    }
    @Override
    public void close() throws IOException {
        if (password != null) {
            Arrays.fill(password, '\u0000');
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (null != username){
            builder.field("username", username);
        }
        if (password != null) {
            byte[] charBytes = CharArrays.toUtf8Bytes(password);
            builder.field("password").utf8Value(charBytes, 0, charBytes.length);
        }
        return builder.endObject();
    }

    @Override
    public Optional<ValidationException> validate() {
        return Optional.empty();
    }
}
