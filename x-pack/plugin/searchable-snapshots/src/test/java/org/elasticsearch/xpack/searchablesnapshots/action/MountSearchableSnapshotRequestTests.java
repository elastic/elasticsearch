/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.master.MasterNodeRequest;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MountSearchableSnapshotRequestTests extends AbstractWireSerializingTestCase<MountSearchableSnapshotRequest> {
    public void testParse() throws Exception {
        final MountSearchableSnapshotRequest original = randomState(createTestInstance());
        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        XContentParser parser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());

        // we will only restore properties from the map that are contained in the request body. All other
        // properties are restored from the original (in the actual REST action this is restored from the
        // REST path and request parameters).
        final MountSearchableSnapshotRequest processed = MountSearchableSnapshotRequest.PARSER.apply(parser,
            new MountSearchableSnapshotRequest.RequestParams(original.mountedIndex(), original.masterNodeTimeout(),
                original.waitForCompletion()));

        assertEquals(original, processed);
    }

    private MountSearchableSnapshotRequest randomState(MountSearchableSnapshotRequest instance) {
        final String mountedIndexName = randomBoolean() ? instance.mountedIndex() : randomAlphaOfLength(5);
        final String repository = randomBoolean() ? instance.repository() : randomAlphaOfLength(5);
        final String snapshotName = randomBoolean() ? instance.snapshot() : randomAlphaOfLength(5);
        final String snapshotIndexName = randomBoolean() ? instance.snapshotIndex() : randomAlphaOfLength(5);

        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(instance.settings());
        } else {
            for (int i = randomInt(3) + 1; i >= 0; i--) {
                settings.put(randomAlphaOfLengthBetween(2, 5), randomAlphaOfLengthBetween(2, 5));
            }
        }

        final Settings.Builder indexSettings = Settings.builder();
        if (randomBoolean()) {
            settings.put(instance.settings());
        } else {
            for (int i = randomInt(3) + 1; i >= 0; i--) {
                settings.put(randomAlphaOfLengthBetween(2, 5), randomAlphaOfLengthBetween(2, 5));
            }
        }

        final List<String> ignoreIndexSettings = new ArrayList<>();
        if  (randomBoolean()) {
            ignoreIndexSettings.addAll(Arrays.asList(instance.ignoreIndexSettings()));
        } else {
            for (int i = randomInt(3) + 1; i >= 0; i--) {
                ignoreIndexSettings.add(randomAlphaOfLengthBetween(2, 5));
            }
        }

        return new MountSearchableSnapshotRequest(mountedIndexName,  repository, snapshotName, snapshotIndexName,
            settings.build(), indexSettings.build(), ignoreIndexSettings.toArray(Strings.EMPTY_ARRAY),
            instance.masterNodeTimeout(), randomBoolean());
    }

    @Override
    protected MountSearchableSnapshotRequest createTestInstance() {
        return randomState(new MountSearchableSnapshotRequest(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5),
            randomAlphaOfLength(5), Settings.EMPTY, Settings.EMPTY, Strings.EMPTY_ARRAY, MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
            randomBoolean()));
    }

    @Override
    protected Writeable.Reader<MountSearchableSnapshotRequest> instanceReader() {
        return MountSearchableSnapshotRequest::new;
    }

    @Override
    protected MountSearchableSnapshotRequest mutateInstance(MountSearchableSnapshotRequest req) {
        switch (randomInt(8)) {
            case 0:
                return new MountSearchableSnapshotRequest(mutateString(req.mountedIndex()), req.repository(), req.snapshot(),
                    req.snapshotIndex(), req.settings(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.masterNodeTimeout(), req.waitForCompletion());
            case 1:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), mutateString(req.repository()), req.snapshot(),
                    req.snapshotIndex(), req.settings(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.masterNodeTimeout(), req.waitForCompletion());
            case 2:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), req.repository(), mutateString(req.snapshot()),
                    req.snapshotIndex(), req.settings(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.masterNodeTimeout(), req.waitForCompletion());
            case 3:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), req.repository(), req.snapshot(),
                    mutateString(req.snapshotIndex()), req.settings(), req.indexSettings(),
                    req.ignoreIndexSettings(), req.masterNodeTimeout(), req.waitForCompletion());
            case 4:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), req.repository(), req.snapshot(),
                    req.snapshotIndex(), mutateSettings(req.settings()), req.indexSettings(), req.ignoreIndexSettings(),
                    req.masterNodeTimeout(), req.waitForCompletion());
            case 5:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), req.repository(), req.snapshot(),
                    req.snapshotIndex(), req.settings(), mutateSettings(req.indexSettings()), req.ignoreIndexSettings(),
                    req.masterNodeTimeout(), req.waitForCompletion());
            case 6:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), req.repository(), req.snapshot(),
                    req.snapshotIndex(), req.settings(), req.indexSettings(), mutateStringArray(req.ignoreIndexSettings()),
                    req.masterNodeTimeout(), req.waitForCompletion());
            case 7:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), req.repository(), req.snapshot(),
                    req.snapshotIndex(), req.settings(), req.indexSettings(), req.ignoreIndexSettings(),
                    mutateTimeValue(req.masterNodeTimeout()), req.waitForCompletion());
            default:
                return new MountSearchableSnapshotRequest(req.mountedIndex(), req.repository(), req.snapshot(),
                    req.snapshotIndex(), req.settings(), req.indexSettings(), req.ignoreIndexSettings(),
                    req.masterNodeTimeout(), req.waitForCompletion() == false);
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
