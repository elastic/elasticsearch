/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MountSearchableSnapshotRequestTests extends AbstractWireSerializingTestCase<MountSearchableSnapshotRequest> {

    private MountSearchableSnapshotRequest randomState(MountSearchableSnapshotRequest instance) {
        return new MountSearchableSnapshotRequest(
            randomBoolean() ? instance.mountedIndexName() : mutateString(instance.mountedIndexName()),
            randomBoolean() ? instance.repositoryName() : mutateString(instance.repositoryName()),
            randomBoolean() ? instance.snapshotName() : mutateString(instance.snapshotName()),
            randomBoolean() ? instance.snapshotIndexName() : mutateString(instance.snapshotIndexName()),
            randomBoolean() ? instance.indexSettings() : mutateSettings(instance.indexSettings()),
            randomBoolean() ? instance.ignoreIndexSettings() : mutateStringArray(instance.ignoreIndexSettings()),
            randomBoolean(),
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        ).masterNodeTimeout(randomBoolean() ? instance.masterNodeTimeout() : mutateTimeValue(instance.masterNodeTimeout()));
    }

    @Override
    protected MountSearchableSnapshotRequest createTestInstance() {
        return randomState(
            new MountSearchableSnapshotRequest(
                randomAlphaOfLength(5),
                randomAlphaOfLength(5),
                randomAlphaOfLength(5),
                randomAlphaOfLength(5),
                Settings.EMPTY,
                Strings.EMPTY_ARRAY,
                randomBoolean(),
                randomFrom(MountSearchableSnapshotRequest.Storage.values())
            )
        );
    }

    @Override
    protected Writeable.Reader<MountSearchableSnapshotRequest> instanceReader() {
        return MountSearchableSnapshotRequest::new;
    }

    @Override
    protected MountSearchableSnapshotRequest mutateInstance(MountSearchableSnapshotRequest req) {
        return switch (randomInt(8)) {
            case 0 -> new MountSearchableSnapshotRequest(
                mutateString(req.mountedIndexName()),
                req.repositoryName(),
                req.snapshotName(),
                req.snapshotIndexName(),
                req.indexSettings(),
                req.ignoreIndexSettings(),
                req.waitForCompletion(),
                req.storage()
            ).masterNodeTimeout(req.masterNodeTimeout());
            case 1 -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                mutateString(req.repositoryName()),
                req.snapshotName(),
                req.snapshotIndexName(),
                req.indexSettings(),
                req.ignoreIndexSettings(),
                req.waitForCompletion(),
                req.storage()
            ).masterNodeTimeout(req.masterNodeTimeout());
            case 2 -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                req.repositoryName(),
                mutateString(req.snapshotName()),
                req.snapshotIndexName(),
                req.indexSettings(),
                req.ignoreIndexSettings(),
                req.waitForCompletion(),
                req.storage()
            ).masterNodeTimeout(req.masterNodeTimeout());
            case 3 -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                req.repositoryName(),
                req.snapshotName(),
                mutateString(req.snapshotIndexName()),
                req.indexSettings(),
                req.ignoreIndexSettings(),
                req.waitForCompletion(),
                req.storage()
            ).masterNodeTimeout(req.masterNodeTimeout());
            case 4 -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                req.repositoryName(),
                req.snapshotName(),
                req.snapshotIndexName(),
                mutateSettings(req.indexSettings()),
                req.ignoreIndexSettings(),
                req.waitForCompletion(),
                req.storage()
            ).masterNodeTimeout(req.masterNodeTimeout());
            case 5 -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                req.repositoryName(),
                req.snapshotName(),
                req.snapshotIndexName(),
                req.indexSettings(),
                mutateStringArray(req.ignoreIndexSettings()),
                req.waitForCompletion(),
                req.storage()
            ).masterNodeTimeout(req.masterNodeTimeout());
            case 6 -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                req.repositoryName(),
                req.snapshotName(),
                req.snapshotIndexName(),
                req.indexSettings(),
                req.ignoreIndexSettings(),
                req.waitForCompletion() == false,
                req.storage()
            ).masterNodeTimeout(req.masterNodeTimeout());
            case 7 -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                req.repositoryName(),
                req.snapshotName(),
                req.snapshotIndexName(),
                req.indexSettings(),
                req.ignoreIndexSettings(),
                req.waitForCompletion(),
                randomValueOtherThan(req.storage(), () -> randomFrom(MountSearchableSnapshotRequest.Storage.values()))
            ).masterNodeTimeout(req.masterNodeTimeout());
            default -> new MountSearchableSnapshotRequest(
                req.mountedIndexName(),
                req.repositoryName(),
                req.snapshotName(),
                req.snapshotIndexName(),
                req.indexSettings(),
                req.ignoreIndexSettings(),
                req.waitForCompletion(),
                req.storage()
            ).masterNodeTimeout(mutateTimeValue(req.masterNodeTimeout()));
        };
    }

    private static TimeValue mutateTimeValue(TimeValue timeValue) {
        long millis = timeValue.millis();
        long newMillis = randomValueOtherThan(millis, () -> randomLongBetween(0, 60000));
        return TimeValue.timeValueMillis(newMillis);
    }

    private static String mutateString(String string) {
        return randomAlphaOfLength(11 - string.length());
    }

    private static Settings mutateSettings(Settings settings) {
        if (settings.size() < 5 && (settings.isEmpty() || randomBoolean())) {
            return Settings.builder().put(settings).put(randomAlphaOfLength(3), randomAlphaOfLength(3)).build();
        } else {
            return Settings.EMPTY;
        }
    }

    private static String[] mutateStringArray(String[] strings) {
        if (strings.length < 5 && (strings.length == 0 || randomBoolean())) {
            String[] newStrings = Arrays.copyOf(strings, strings.length + 1);
            newStrings[strings.length] = randomAlphaOfLength(3);
            return newStrings;
        } else if (randomBoolean()) {
            String[] newStrings = Arrays.copyOf(strings, strings.length);
            int i = randomIntBetween(0, newStrings.length - 1);
            newStrings[i] = mutateString(newStrings[i]);
            return newStrings;
        } else {
            return Strings.EMPTY_ARRAY;
        }
    }

    public void testForbidsCustomDataPath() {
        final ActionRequestValidationException validationException = new MountSearchableSnapshotRequest(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            Settings.builder().put(IndexMetadata.SETTING_DATA_PATH, randomAlphaOfLength(5)).build(),
            Strings.EMPTY_ARRAY,
            randomBoolean(),
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        ).validate();
        assertThat(validationException.getMessage(), containsString(IndexMetadata.SETTING_DATA_PATH));
    }

    public void testParsesStorage() throws IOException {
        assertStorageParser("full_copy", MountSearchableSnapshotRequest.Storage.FULL_COPY);
        assertStorageParser("shared_cache", MountSearchableSnapshotRequest.Storage.SHARED_CACHE);
        assertStorageParser("FULL_COPY", MountSearchableSnapshotRequest.Storage.FULL_COPY);
    }

    private static void assertStorageParser(String storageParam, MountSearchableSnapshotRequest.Storage expected) throws IOException {
        final Map<String, String> params = new HashMap<>();
        params.put("repository", "test");
        params.put("snapshot", "test");
        params.put("storage", storageParam);

        final RestRequest restReq = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params)
            .withContent(new BytesArray("{\"index\":\"test\"}"), XContentType.JSON)
            .build();
        final MountSearchableSnapshotRequest mountReq = MountSearchableSnapshotRequest.PARSER.apply(restReq.contentParser(), restReq);
        assertThat(mountReq.storage(), equalTo(expected));
    }

}
