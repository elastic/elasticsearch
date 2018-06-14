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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
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
            this.secureSettingsPassword = new SecureString(utf8BytesToChars(passwordBytes));
        } finally {
            Arrays.fill(passwordBytes, (byte) 0);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        final byte[] passwordBytes = charsToUtf8Bytes(this.secureSettingsPassword.getChars());
        try {
            out.writeByteArray(passwordBytes);
        } finally {
            Arrays.fill(passwordBytes, (byte) 0);
        }
    }

    /**
     * Encodes the provided char[] to a UTF-8 byte[]. This is done while avoiding
     * conversions to String. The provided char[] is not modified by this method, so
     * the caller needs to take care of clearing the value if it is sensitive.
     */
    private static byte[] charsToUtf8Bytes(char[] chars) {
        final CharBuffer charBuffer = CharBuffer.wrap(chars);
        final ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        final byte[] bytes;
        if (byteBuffer.hasArray()) {
            // there is no guarantee that the byte buffers backing array is the right size
            // so we need to make a copy
            bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
            Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
        } else {
            final int length = byteBuffer.limit() - byteBuffer.position();
            bytes = new byte[length];
            byteBuffer.get(bytes);
            // if the buffer is not read only we can reset and fill with 0's
            if (byteBuffer.isReadOnly() == false) {
                byteBuffer.clear(); // reset
                for (int i = 0; i < byteBuffer.limit(); i++) {
                    byteBuffer.put((byte) 0);
                }
            }
        }
        return bytes;
    }

    /**
     * Decodes the provided byte[] to a UTF-8 char[]. This is done while avoiding
     * conversions to String. The provided byte[] is not modified by this method, so
     * the caller needs to take care of clearing the value if it is sensitive.
     */
    public static char[] utf8BytesToChars(byte[] utf8Bytes) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(utf8Bytes);
        final CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
        final char[] chars;
        if (charBuffer.hasArray()) {
            // there is no guarantee that the char buffers backing array is the right size
            // so we need to make a copy
            chars = Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit());
            Arrays.fill(charBuffer.array(), (char) 0); // clear sensitive data
        } else {
            final int length = charBuffer.limit() - charBuffer.position();
            chars = new char[length];
            charBuffer.get(chars);
            // if the buffer is not read only we can reset and fill with 0's
            if (charBuffer.isReadOnly() == false) {
                charBuffer.clear(); // reset
                for (int i = 0; i < charBuffer.limit(); i++) {
                    charBuffer.put((char) 0);
                }
            }
        }
        return chars;
    }
}
