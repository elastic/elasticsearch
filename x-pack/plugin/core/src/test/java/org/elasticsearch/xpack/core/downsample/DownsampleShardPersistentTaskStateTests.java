/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DownsampleShardPersistentTaskStateTests extends AbstractXContentSerializingTestCase<DownsampleShardPersistentTaskState> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskState.class,
                    DownsampleShardPersistentTaskState.NAME,
                    DownsampleShardPersistentTaskState::readFromStream
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<DownsampleShardPersistentTaskState> instanceReader() {
        return DownsampleShardPersistentTaskState::new;
    }

    @Override
    protected DownsampleShardPersistentTaskState createTestInstance() {
        try {
            return createRollupShardPersistentTaskState();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DownsampleShardPersistentTaskState createRollupShardPersistentTaskState() throws IOException {
        return new DownsampleShardPersistentTaskState(
            randomFrom(DownsampleShardIndexerStatus.values()),
            randomTsid(),
            randomTargetIndexUuid()
        );
    }

    private static String randomTargetIndexUuid() {
        return randomBoolean() ? null : UUIDs.randomBase64UUID();
    }

    /**
     * The tsid is a hash, so it must survive a round trip as arbitrary bytes, and it is absent before anything was indexed.
     */
    private static BytesRef randomTsid() {
        return randomBoolean() ? null : new BytesRef(randomByteArrayOfLength(randomIntBetween(1, 64)));
    }

    @Override
    protected DownsampleShardPersistentTaskState mutateInstance(DownsampleShardPersistentTaskState instance) throws IOException {
        DownsampleShardIndexerStatus status = instance.downsampleShardIndexerStatus();
        BytesRef tsid = instance.tsid();
        String targetIndexUuid = instance.targetIndexUuid();
        switch (between(0, 2)) {
            case 0 -> status = randomValueOtherThan(status, () -> randomFrom(DownsampleShardIndexerStatus.values()));
            case 1 -> tsid = randomValueOtherThan(tsid, DownsampleShardPersistentTaskStateTests::randomTsid);
            case 2 -> targetIndexUuid = randomValueOtherThan(
                targetIndexUuid,
                DownsampleShardPersistentTaskStateTests::randomTargetIndexUuid
            );
            default -> throw new AssertionError("unexpected mutation");
        }
        return new DownsampleShardPersistentTaskState(status, tsid, targetIndexUuid);
    }

    @Override
    protected DownsampleShardPersistentTaskState doParseInstance(XContentParser parser) throws IOException {
        return DownsampleShardPersistentTaskState.fromXContent(parser);
    }

    /**
     * Nodes before {@link DownsampleShardPersistentTaskState#DOWNSAMPLE_RESUME_WATERMARK_TARGET_UUID} do not know the target
     * index UUID, so it is not sent to them and the rest of the state is unchanged.
     */
    public void testTargetIndexUuidIsNotSentToOlderVersions() throws IOException {
        DownsampleShardPersistentTaskState instance = new DownsampleShardPersistentTaskState(
            randomFrom(DownsampleShardIndexerStatus.values()),
            randomTsid(),
            UUIDs.randomBase64UUID()
        );
        TransportVersion olderVersion = TransportVersionUtils.getPreviousVersion(
            DownsampleShardPersistentTaskState.DOWNSAMPLE_RESUME_WATERMARK_TARGET_UUID
        );
        DownsampleShardPersistentTaskState copy = copyInstance(instance, olderVersion);
        assertThat(copy.downsampleShardIndexerStatus(), equalTo(instance.downsampleShardIndexerStatus()));
        assertThat(copy.tsid(), equalTo(instance.tsid()));
        assertThat(copy.targetIndexUuid(), nullValue());
    }

    /**
     * The tsid is written under its own field, in the encoding of the {@code _tsid} field, and never under the legacy text field.
     */
    public void testToXContent() {
        var state = new DownsampleShardPersistentTaskState(
            DownsampleShardIndexerStatus.STARTED,
            new BytesRef(new byte[] { 1, 2, 3 }),
            "uuid"
        );
        assertThat(Strings.toString(state), equalTo("""
            {"status":"STARTED","encoded_tsid":"AQID","target_index_uuid":"uuid"}"""));
    }

    /**
     * Earlier versions wrote the tsid as UTF-8 text under the {@code tsid} field, which mangles a hash. Such a value is accepted,
     * so that the cluster state persisted before an upgrade can still be read, but it carries no usable resume point.
     */
    public void testLegacyTsidIsIgnored() throws IOException {
        String json = """
            {"status": "STARTED", "tsid": "some text"}""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DownsampleShardPersistentTaskState state = DownsampleShardPersistentTaskState.fromXContent(parser);
            assertThat(state.downsampleShardIndexerStatus(), equalTo(DownsampleShardIndexerStatus.STARTED));
            assertThat(state.tsid(), nullValue());
        }
    }
}
