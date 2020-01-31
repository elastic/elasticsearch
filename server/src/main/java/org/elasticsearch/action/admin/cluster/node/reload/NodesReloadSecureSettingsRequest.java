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

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;

import java.util.Arrays;

/**
 * Request for a reload secure settings action
 */
public class NodesReloadSecureSettingsRequest extends BaseNodesRequest<NodesReloadSecureSettingsRequest> {

    /**
     * The password is used to re-read and decrypt the contents
     * of the node's keystore (backing the implementation of
     * {@code SecureSettings}).
     */
    @Nullable
    private SecureString secureSettingsPassword;

    public NodesReloadSecureSettingsRequest() {
        super((String[]) null);
    }

    public NodesReloadSecureSettingsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            final BytesReference bytesRef = in.readOptionalBytesReference();
            if (bytesRef != null) {
                byte[] bytes = BytesReference.toBytes(bytesRef);
                try {
                    this.secureSettingsPassword = new SecureString(CharArrays.utf8BytesToChars(bytes));
                } finally {
                    Arrays.fill(bytes, (byte) 0);
                }
            } else {
                this.secureSettingsPassword = null;
            }
        }
    }

    /**
     * Reload secure settings only on certain nodes, based on the nodes ids
     * specified. If none are passed, secure settings will be reloaded on all the
     * nodes.
     */
    public NodesReloadSecureSettingsRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Nullable
    public SecureString getSecureSettingsPassword() {
        return secureSettingsPassword;
    }

    public void setSecureStorePassword(SecureString secureStorePassword) {
        this.secureSettingsPassword = secureStorePassword;
    }

    public void closePassword() {
        if (this.secureSettingsPassword != null) {
            this.secureSettingsPassword.close();
        }
    }

    boolean hasPassword() {
        return this.secureSettingsPassword != null && this.secureSettingsPassword.length() > 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            if (this.secureSettingsPassword == null) {
                out.writeOptionalBytesReference(null);
            } else {
                final byte[] passwordBytes = CharArrays.toUtf8Bytes(this.secureSettingsPassword.getChars());
                try {
                    out.writeOptionalBytesReference(new BytesArray(passwordBytes));
                } finally {
                    Arrays.fill(passwordBytes, (byte) 0);
                }
            }
        }
    }
}
