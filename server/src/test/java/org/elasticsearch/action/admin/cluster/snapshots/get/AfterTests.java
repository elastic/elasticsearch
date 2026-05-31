/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.elasticsearch.action.admin.cluster.snapshots.get.After.fromSnapshotInfo;
import static org.elasticsearch.snapshots.SnapshotInfoTestUtils.createRandomSnapshotInfo;
import static org.hamcrest.Matchers.equalTo;

public class AfterTests extends AbstractWireSerializingTestCase<After> {

    @Override
    protected Writeable.Reader<After> instanceReader() {
        return After::new;
    }

    @Override
    protected After createTestInstance() {
        return new After(randomAlphaOfLengthBetween(1, 20), randomRepoName(), randomSnapshotName());
    }

    @Override
    protected After mutateInstance(final After instance) {
        return switch (between(0, 2)) {
            case 0 -> new After(
                randomValueOtherThan(instance.value(), () -> randomAlphaOfLengthBetween(1, 20)),
                instance.repoName(),
                instance.snapshotName()
            );
            case 1 -> new After(
                instance.value(),
                randomValueOtherThan(instance.repoName(), ESTestCase::randomRepoName),
                instance.snapshotName()
            );
            case 2 -> new After(
                instance.value(),
                instance.repoName(),
                randomValueOtherThan(instance.snapshotName(), ESTestCase::randomSnapshotName)
            );
            default -> throw new AssertionError("impossible");
        };
    }

    public void testRoundTripToQueryParam() {
        final var after = createTestInstance();
        assertThat(After.decodeAfterQueryParam(after.toQueryParam()), equalTo(after));
    }

    public void testDecodeAfterQueryParamInvalidFormatThrows() {
        expectThrows(IllegalArgumentException.class, () -> After.decodeAfterQueryParam("not-valid-base64!!!"));
        expectThrows(
            IllegalArgumentException.class,
            () -> After.decodeAfterQueryParam(Base64.getUrlEncoder().encodeToString("only,two".getBytes(StandardCharsets.UTF_8)))
        );
    }

    public void testFromSnapshotInfo() {
        final var info = createRandomSnapshotInfo();
        assertThat(fromSnapshotInfo(info, randomFrom(SnapshotSortKey.values())).repoName(), equalTo(info.repository()));
        assertThat(fromSnapshotInfo(info, randomFrom(SnapshotSortKey.values())).snapshotName(), equalTo(info.snapshotId().getName()));
        assertThat(fromSnapshotInfo(info, SnapshotSortKey.START_TIME).value(), equalTo(Long.toString(info.startTime())));
        assertThat(fromSnapshotInfo(info, SnapshotSortKey.NAME).value(), equalTo(info.snapshotId().getName()));
        assertThat(fromSnapshotInfo(info, SnapshotSortKey.DURATION).value(), equalTo(Long.toString(info.endTime() - info.startTime())));
        assertThat(fromSnapshotInfo(info, SnapshotSortKey.INDICES).value(), equalTo(Integer.toString(info.indices().size())));
        assertThat(fromSnapshotInfo(info, SnapshotSortKey.SHARDS).value(), equalTo(Integer.toString(info.totalShards())));
        assertThat(fromSnapshotInfo(info, SnapshotSortKey.FAILED_SHARDS).value(), equalTo(Integer.toString(info.failedShards())));
        assertThat(fromSnapshotInfo(info, SnapshotSortKey.REPOSITORY).value(), equalTo(info.repository()));
    }
}
