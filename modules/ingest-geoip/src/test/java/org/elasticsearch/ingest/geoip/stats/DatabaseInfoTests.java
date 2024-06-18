/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class DatabaseInfoTests extends AbstractWireSerializingTestCase<DatabaseInfo> {
    @Override
    protected Writeable.Reader<DatabaseInfo> instanceReader() {
        return DatabaseInfo::new;
    }

    @Override
    protected DatabaseInfo createTestInstance() {
        return new DatabaseInfo(randomName(), randomSource(), randomMd5(), randomMd5(), randomBuildDate(), randomType());
    }

    @Override
    protected DatabaseInfo mutateInstance(DatabaseInfo instance) throws IOException {
        return switch (between(0, 5)) {
            case 0 -> new DatabaseInfo(
                randomValueOtherThan(instance.name(), this::randomName),
                instance.source(),
                instance.archiveMd5(),
                instance.md5(),
                instance.buildDateInMillis(),
                instance.type()
            );
            case 1 -> new DatabaseInfo(
                instance.name(),
                randomValueOtherThan(instance.source(), this::randomSource),
                instance.archiveMd5(),
                instance.md5(),
                instance.buildDateInMillis(),
                instance.type()
            );
            case 2 -> new DatabaseInfo(
                instance.name(),
                instance.source(),
                randomValueOtherThan(instance.archiveMd5(), this::randomMd5),
                instance.md5(),
                instance.buildDateInMillis(),
                instance.type()
            );
            case 3 -> new DatabaseInfo(
                instance.name(),
                instance.source(),
                instance.archiveMd5(),
                randomValueOtherThan(instance.md5(), this::randomMd5),
                instance.buildDateInMillis(),
                instance.type()
            );
            case 4 -> new DatabaseInfo(
                instance.name(),
                instance.source(),
                instance.archiveMd5(),
                instance.md5(),
                randomValueOtherThan(instance.buildDateInMillis(), this::randomBuildDate),
                instance.type()
            );
            case 5 -> new DatabaseInfo(
                instance.name(),
                instance.source(),
                instance.archiveMd5(),
                instance.md5(),
                instance.buildDateInMillis(),
                randomValueOtherThan(instance.type(), this::randomType)
            );
            default -> throw new AssertionError("Should never get here");
        };
    }

    private String randomName() {
        return randomAlphaOfLengthBetween(5, 30);
    }

    private String randomSource() {
        return randomBoolean() ? null : randomAlphaOfLength(10);
    }

    private String randomMd5() {
        return randomBoolean() ? null : randomAlphaOfLength(20);
    }

    private Long randomBuildDate() {
        return randomBoolean() ? null : randomLong();
    }

    private String randomType() {
        return randomBoolean() ? null : randomAlphaOfLength(100);
    }
}
