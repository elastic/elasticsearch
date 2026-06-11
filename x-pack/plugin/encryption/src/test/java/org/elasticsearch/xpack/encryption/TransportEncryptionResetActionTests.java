/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TransportEncryptionResetActionTests extends ESTestCase {

    public void testApplyDestructiveResetRemovesCustomWhenHandlerReturnsNull() {
        ProjectId projectId = ProjectId.DEFAULT;
        TestCustom existing = new TestCustom("encrypted-blob");
        ProjectMetadata project = ProjectMetadata.builder(projectId).putCustom(TestCustom.TYPE, existing).build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);

        TransportEncryptionResetAction.applyDestructiveReset(builder, project, new TestHandler(unused -> null));

        assertNull(builder.build().custom(TestCustom.TYPE));
    }

    public void testApplyDestructiveResetReplacesCustomWhenHandlerReturnsNewValue() {
        ProjectId projectId = ProjectId.DEFAULT;
        TestCustom existing = new TestCustom("encrypted-blob");
        TestCustom replacement = new TestCustom("rebuilt-cleartext");
        ProjectMetadata project = ProjectMetadata.builder(projectId).putCustom(TestCustom.TYPE, existing).build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);

        TransportEncryptionResetAction.applyDestructiveReset(builder, project, new TestHandler(unused -> replacement));

        assertSame(replacement, builder.build().custom(TestCustom.TYPE));
    }

    public void testApplyDestructiveResetLeavesCustomWhenHandlerReturnsCurrent() {
        ProjectId projectId = ProjectId.DEFAULT;
        TestCustom existing = new TestCustom("encrypted-blob");
        ProjectMetadata project = ProjectMetadata.builder(projectId).putCustom(TestCustom.TYPE, existing).build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);

        TransportEncryptionResetAction.applyDestructiveReset(builder, project, new TestHandler(current -> current));

        assertSame(existing, builder.build().custom(TestCustom.TYPE));
    }

    public void testApplyDestructiveResetWithMissingCustomAndNullReturnIsNoop() {
        ProjectId projectId = ProjectId.DEFAULT;
        ProjectMetadata project = ProjectMetadata.builder(projectId).build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);

        TransportEncryptionResetAction.applyDestructiveReset(builder, project, new TestHandler(unused -> null));

        assertNull(builder.build().custom(TestCustom.TYPE));
    }

    public void testApplyDestructiveResetCreatesCustomWhenAbsentAndHandlerReturnsValue() {
        ProjectId projectId = ProjectId.DEFAULT;
        TestCustom newValue = new TestCustom("fresh-cleartext");
        ProjectMetadata project = ProjectMetadata.builder(projectId).build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);

        TransportEncryptionResetAction.applyDestructiveReset(builder, project, new TestHandler(unused -> newValue));

        assertSame(newValue, builder.build().custom(TestCustom.TYPE));
    }

    public void testApplyDestructiveResetThrowsWhenHandlerReturnsCustomWithMismatchedName() {
        ProjectId projectId = ProjectId.DEFAULT;
        TestCustom existing = new TestCustom("data");
        ProjectMetadata project = ProjectMetadata.builder(projectId).putCustom(TestCustom.TYPE, existing).build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);

        // Handler claims to own "other_type" but onDestructiveReset returns a TestCustom
        // whose getWriteableName() == "test_encrypted_custom".
        EncryptedDataHandler<TestCustom> badHandler = new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return "other_type";
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, EncryptionService encryptionService, String activeKeyId) {
                return current;
            }

            @Override
            public TestCustom onDestructiveReset(TestCustom current) {
                return new TestCustom("value");
            }
        };

        expectThrows(IllegalStateException.class, () -> TransportEncryptionResetAction.applyDestructiveReset(builder, project, badHandler));
    }

    public void testExecuteResetRemovesPekCustom() {
        ProjectEncryptionKeyMetadata pek = somePek();
        ClusterState state = stateWithPek(pek);
        var task = resetTask(List.of());

        ClusterState result = TransportEncryptionResetAction.executeReset(state, task);

        assertNull(result.metadata().getProject(ProjectId.DEFAULT).custom(ProjectEncryptionKeyMetadata.TYPE));
    }

    public void testExecuteResetWithNoPekDoesNotThrow() {
        ClusterState state = stateWithPek(null);
        var task = resetTask(List.of());

        ClusterState result = TransportEncryptionResetAction.executeReset(state, task);

        assertNull(result.metadata().getProject(ProjectId.DEFAULT).custom(ProjectEncryptionKeyMetadata.TYPE));
    }

    public void testExecuteResetInvokesHandlerAndPekIsAlsoRemoved() {
        ProjectEncryptionKeyMetadata pek = somePek();
        TestCustom existing = new TestCustom("encrypted-blob");
        TestCustom replacement = new TestCustom("rebuilt-cleartext");
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(ProjectId.DEFAULT);
        projectBuilder.putCustom(ProjectEncryptionKeyMetadata.TYPE, pek);
        projectBuilder.putCustom(TestCustom.TYPE, existing);
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(projectBuilder.build()).build())
            .build();

        var task = resetTask(List.of(new TestHandler(unused -> replacement)));
        ClusterState result = TransportEncryptionResetAction.executeReset(state, task);

        ProjectMetadata resultProject = result.metadata().getProject(ProjectId.DEFAULT);
        assertNull(resultProject.custom(ProjectEncryptionKeyMetadata.TYPE));
        assertSame(replacement, resultProject.custom(TestCustom.TYPE));
    }

    public void testHandlerRegistryExposesContributedHandlers() {
        EncryptedDataHandler<?> handler = new TestHandler(unused -> null);
        EncryptedDataHandlerRegistry registry = new EncryptedDataHandlerRegistry(List.of(handler));
        assertEquals(1, registry.handlers().size());
        assertSame(handler, registry.handlers().get(0));
    }

    private static ClusterState stateWithPek(ProjectEncryptionKeyMetadata pek) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(ProjectId.DEFAULT);
        if (pek != null) {
            project.putCustom(ProjectEncryptionKeyMetadata.TYPE, pek);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project.build()).build()).build();
    }

    private static ProjectEncryptionKeyMetadata somePek() {
        byte[] key = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(key);
        return new ProjectEncryptionKeyMetadata(Map.of("k1", new KeyEntry(key, 42L)), "k1", "v1");
    }

    private static TransportEncryptionResetAction.EncryptionResetTask resetTask(List<EncryptedDataHandler<?>> handlers) {
        return new TransportEncryptionResetAction.EncryptionResetTask(ProjectId.DEFAULT, handlers, TimeValue.ZERO, ActionListener.noop());
    }

    @FunctionalInterface
    private interface ResetFn {
        TestCustom apply(TestCustom current);
    }

    private record TestHandler(ResetFn resetFn) implements EncryptedDataHandler<TestCustom> {
        @Override
        public String customName() {
            return TestCustom.TYPE;
        }

        @Override
        public TestCustom reEncrypt(TestCustom current, EncryptionService encryptionService, String activeKeyId) {
            return current;
        }

        @Override
        public TestCustom onDestructiveReset(TestCustom current) {
            return resetFn.apply(current);
        }
    }

    /**
     * Minimal {@link Metadata.ProjectCustom} for verifying the destructive-reset state mutation; carries one string so equality
     * and instance identity are easy to assert against.
     */
    private static final class TestCustom extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

        static final String TYPE = "test_encrypted_custom";

        private final String value;

        TestCustom(String value) {
            this.value = Objects.requireNonNull(value);
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
            out.writeString(value);
        }

        public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("value", value));
        }
    }
}
