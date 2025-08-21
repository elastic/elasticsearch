/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.LeakTracker;

import java.io.IOException;
import java.util.Arrays;

/**
 * Request for a reload secure settings action
 */
public class NodesReloadSecureSettingsRequest extends BaseNodesRequest {

    /**
     * The password is used to re-read and decrypt the contents
     * of the node's keystore (backing the implementation of
     * {@code SecureSettings}).
     */
    @Nullable
    private SecureString secureSettingsPassword;

    private final RefCounted refs = LeakTracker.wrap(AbstractRefCounted.of(() -> Releasables.close(secureSettingsPassword)));

    public NodesReloadSecureSettingsRequest(String[] nodeIds) {
        super(nodeIds);
    }

    public void setSecureStorePassword(SecureString secureStorePassword) {
        this.secureSettingsPassword = secureStorePassword;
    }

    boolean hasPassword() {
        return this.secureSettingsPassword != null && this.secureSettingsPassword.length() > 0;
    }

    @Override
    public void incRef() {
        refs.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refs.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refs.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refs.hasReferences();
    }

    NodeRequest newNodeRequest() {
        refs.mustIncRef();
        return new NodeRequest(secureSettingsPassword, refs);
    }

    public static class NodeRequest extends AbstractTransportRequest {

        @Nullable
        private final SecureString secureSettingsPassword;

        private final RefCounted refs;

        NodeRequest(StreamInput in) throws IOException {
            super(in);

            if (in.getTransportVersion().before(TransportVersions.V_8_13_0)) {
                TaskId.readFromStream(in);
                in.readStringArray();
                in.readOptionalArray(DiscoveryNode::new, DiscoveryNode[]::new);
                in.readOptionalTimeValue();
            }

            final BytesReference bytesRef = in.readOptionalBytesReference();
            if (bytesRef != null) {
                byte[] bytes = BytesReference.toBytes(bytesRef);
                try {
                    this.secureSettingsPassword = new SecureString(CharArrays.utf8BytesToChars(bytes));
                    this.refs = LeakTracker.wrap(AbstractRefCounted.of(() -> Releasables.close(this.secureSettingsPassword)));
                } finally {
                    Arrays.fill(bytes, (byte) 0);
                }
            } else {
                this.secureSettingsPassword = null;
                this.refs = LeakTracker.wrap(AbstractRefCounted.of(() -> {}));
            }
        }

        NodeRequest(@Nullable SecureString secureSettingsPassword, RefCounted refs) {
            assert secureSettingsPassword == null || secureSettingsPassword.getChars() != null; // ensures it's not closed
            assert refs.hasReferences();
            this.secureSettingsPassword = secureSettingsPassword;
            this.refs = refs;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert hasReferences();
            super.writeTo(out);

            if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
                TaskId.EMPTY_TASK_ID.writeTo(out);
                out.writeStringArray(Strings.EMPTY_ARRAY);
                out.writeOptionalArray(StreamOutput::writeWriteable, null);
                out.writeOptionalTimeValue(null);
            }

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

        boolean hasPassword() {
            assert hasReferences();
            return this.secureSettingsPassword != null && this.secureSettingsPassword.length() > 0;
        }

        @Nullable
        public SecureString getSecureSettingsPassword() {
            assert hasReferences();
            return secureSettingsPassword;
        }

        @Override
        public void incRef() {
            refs.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refs.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refs.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refs.hasReferences();
        }
    }
}
