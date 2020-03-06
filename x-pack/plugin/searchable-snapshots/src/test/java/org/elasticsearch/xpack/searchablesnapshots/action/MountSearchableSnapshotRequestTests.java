/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Arrays;
import java.util.Collections;

public class MountSearchableSnapshotRequestTests extends AbstractWireSerializingTestCase<MountSearchableSnapshotRequest> {
    public void testParse() throws Exception {
        final MountSearchableSnapshotRequest original = randomState(createTestInstance());
        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        XContentParser parser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());

        final MountSearchableSnapshotRequest processed = MountSearchableSnapshotRequest.PARSER.apply(parser,
            new MountSearchableSnapshotRequest.RequestParams(original.mountedIndexName(), original.waitForCompletion()))
            .masterNodeTimeout(original.masterNodeTimeout());

        assertEquals(original, processed);
    }

    private MountSearchableSnapshotRequest randomState(MountSearchableSnapshotRequest instance) {
        return new MountSearchableSnapshotRequest(
            randomBoolean() ? instance.mountedIndexName() : mutateString(instance.mountedIndexName()),
            randomBoolean() ? instance.repositoryName() : mutateString(instance.repositoryName()),
            randomBoolean() ? instance.snapshotName() : mutateString(instance.snapshotName()),
            randomBoolean() ? instance.snapshotIndexName() : mutateString(instance.snapshotIndexName()),
            randomBoolean() ? instance.indexSettings() : mutateSettings(instance.indexSettings()),
            randomBoolean() ? instance.ignoreIndexSettings() : mutateStringArray(instance.ignoreIndexSettings()),
            randomBoolean())
            .masterNodeTimeout(randomBoolean() ? instance.masterNodeTimeout() : mutateTimeValue(instance.masterNodeTimeout()));
    }

    @Override
    protected MountSearchableSnapshotRequest createTestInstance() {
        return randomState(new MountSearchableSnapshotRequest(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5),
            randomAlphaOfLength(5), Settings.EMPTY, Strings.EMPTY_ARRAY, randomBoolean()));
    }

    @Override
    protected Writeable.Reader<MountSearchableSnapshotRequest> instanceReader() {
        return MountSearchableSnapshotRequest::new;
    }

    @Override
    protected MountSearchableSnapshotRequest mutateInstance(MountSearchableSnapshotRequest req) {
        switch (randomInt(7)) {
            case 0:
                return new MountSearchableSnapshotRequest(mutateString(req.mountedIndexName()), req.repositoryName(), req.snapshotName(),
                    req.snapshotIndexName(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.waitForCompletion()).masterNodeTimeout(req.masterNodeTimeout());
            case 1:
                return new MountSearchableSnapshotRequest(req.mountedIndexName(), mutateString(req.repositoryName()), req.snapshotName(),
                    req.snapshotIndexName(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.waitForCompletion()).masterNodeTimeout(req.masterNodeTimeout());
            case 2:
                return new MountSearchableSnapshotRequest(req.mountedIndexName(), req.repositoryName(), mutateString(req.snapshotName()),
                    req.snapshotIndexName(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.waitForCompletion()).masterNodeTimeout(req.masterNodeTimeout());
            case 3:
                return new MountSearchableSnapshotRequest(req.mountedIndexName(), req.repositoryName(), req.snapshotName(),
                    mutateString(req.snapshotIndexName()), req.indexSettings(), req.ignoreIndexSettings(),
                    req.waitForCompletion()).masterNodeTimeout(req.masterNodeTimeout());
            case 4:
                return new MountSearchableSnapshotRequest(req.mountedIndexName(), req.repositoryName(), req.snapshotName(),
                    req.snapshotIndexName(), mutateSettings(req.indexSettings()), req.ignoreIndexSettings(),
                    req.waitForCompletion()).masterNodeTimeout(req.masterNodeTimeout());
            case 5:
                return new MountSearchableSnapshotRequest(req.mountedIndexName(), req.repositoryName(), req.snapshotName(),
                    req.snapshotIndexName(), req.indexSettings(), mutateStringArray(req.ignoreIndexSettings()),
                    req.waitForCompletion()).masterNodeTimeout(req.masterNodeTimeout());
            case 6:
                return new MountSearchableSnapshotRequest(req.mountedIndexName(), req.repositoryName(), req.snapshotName(),
                    req.snapshotIndexName(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.waitForCompletion() == false).masterNodeTimeout(req.masterNodeTimeout());

            default:
                return new MountSearchableSnapshotRequest(req.mountedIndexName(), req.repositoryName(), req.snapshotName(),
                    req.snapshotIndexName(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.waitForCompletion()).masterNodeTimeout(mutateTimeValue(req.masterNodeTimeout()));
        }
    }

    private static TimeValue mutateTimeValue(TimeValue timeValue) {
        long millis = timeValue.millis();
        long newMillis = randomValueOtherThan(millis, () ->  randomLongBetween(0, 60000));
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
}
