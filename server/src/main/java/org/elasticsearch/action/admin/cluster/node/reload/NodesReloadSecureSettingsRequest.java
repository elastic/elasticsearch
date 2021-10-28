/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
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
