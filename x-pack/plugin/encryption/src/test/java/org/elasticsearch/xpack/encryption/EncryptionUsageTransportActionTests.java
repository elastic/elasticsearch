/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.junit.After;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

public class EncryptionUsageTransportActionTests extends ESTestCase {

    @After
    public void resetRegistry() {
        EncryptedDataHandlerRegistry.reset();
    }

    public void testClusterHasEncryptedData() {
        EncryptedDataHandlerRegistry.setInstance(new EncryptedDataHandlerRegistry(List.of(new SecretHandler())));

        assertFalse("no custom at all", EncryptionUsageTransportAction.clusterHasEncryptedData(projectWith(null)));
        assertFalse(
            "custom without encrypted values",
            EncryptionUsageTransportAction.clusterHasEncryptedData(projectWith(new SecretCustom(null)))
        );
        assertTrue(
            "custom holding an encrypted value",
            EncryptionUsageTransportAction.clusterHasEncryptedData(
                projectWith(new SecretCustom(new EncryptedData("key", randomByteArrayOfLength(8))))
            )
        );
    }

    private static ProjectMetadata projectWith(@Nullable SecretCustom custom) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        if (custom != null) {
            builder.putCustom(SecretCustom.TYPE, custom);
        }
        return builder.build();
    }

    private static final class SecretHandler implements EncryptedDataHandler<SecretCustom> {
        @Override
        public String customName() {
            return SecretCustom.TYPE;
        }

        @Override
        public SecretCustom reEncrypt(SecretCustom current, UnaryOperator<EncryptedData> reEncrypt) {
            if (current.secret == null) {
                return current;
            }
            EncryptedData rewrapped = reEncrypt.apply(current.secret);
            return rewrapped == current.secret ? current : new SecretCustom(rewrapped);
        }
    }

    /** Minimal project custom holding one optional secret. */
    private static final class SecretCustom extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        static final String TYPE = "usage_test_secret";

        @Nullable
        private final EncryptedData secret;

        SecretCustom(@Nullable EncryptedData secret) {
            this.secret = secret;
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(secret);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.ALL_CONTEXTS;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("secret", secret));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof SecretCustom other && Objects.equals(secret, other.secret);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(secret);
        }
    }
}
