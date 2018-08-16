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


import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for a reload secure settings action
 */
public class NodesReloadSecureSettingsRequest extends BaseNodesRequest<NodesReloadSecureSettingsRequest> {

    /**
     * The password which is broadcasted to all nodes, but is never stored on
     * persistent storage. The password is used to reread and decrypt the contents
     * of the node's keystore (backing the implementation of
     * {@code SecureSettings}).
     */
    private SecureString secureSettingsPassword;

    public NodesReloadSecureSettingsRequest() {
    }

    /**
     * Reload secure settings only on certain nodes, based on the nodes ids
     * specified. If none are passed, secure settings will be reloaded on all the
     * nodes.
     */
    public NodesReloadSecureSettingsRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (secureSettingsPassword == null) {
            validationException = addValidationError("secure settings password cannot be null (use empty string instead)",
                    validationException);
        }
        return validationException;
    }

    public SecureString secureSettingsPassword() {
        return secureSettingsPassword;
    }

    public NodesReloadSecureSettingsRequest secureStorePassword(SecureString secureStorePassword) {
        this.secureSettingsPassword = secureStorePassword;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        final byte[] passwordBytes = in.readByteArray();
        try {
            this.secureSettingsPassword = new SecureString(CharArrays.utf8BytesToChars(passwordBytes));
        } finally {
            Arrays.fill(passwordBytes, (byte) 0);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        final byte[] passwordBytes = CharArrays.toUtf8Bytes(this.secureSettingsPassword.getChars());
        try {
            out.writeByteArray(passwordBytes);
        } finally {
            Arrays.fill(passwordBytes, (byte) 0);
        }
    }
}
