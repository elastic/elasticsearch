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

public class DownloadedDatabaseInfoTests extends AbstractWireSerializingTestCase<DownloadedDatabaseInfo> {
    @Override
    protected Writeable.Reader<DownloadedDatabaseInfo> instanceReader() {
        return DownloadedDatabaseInfo::new;
    }

    @Override
    protected DownloadedDatabaseInfo createTestInstance() {
        return new DownloadedDatabaseInfo(randomName(), randomSuccessfulAttempt(), randomFailureAttempt());
    }

    @Override
    protected DownloadedDatabaseInfo mutateInstance(DownloadedDatabaseInfo instance) throws IOException {
        return switch (between(0, 2)) {
            case 0 -> new DownloadedDatabaseInfo(
                randomValueOtherThan(instance.name(), this::randomName),
                instance.successfulAttempt(),
                instance.failedAttempt()
            );
            case 1 -> new DownloadedDatabaseInfo(
                instance.name(),
                randomValueOtherThan(instance.successfulAttempt(), this::randomSuccessfulAttempt),
                instance.failedAttempt()
            );
            case 2 -> new DownloadedDatabaseInfo(
                instance.name(),
                instance.successfulAttempt(),
                randomValueOtherThan(instance.failedAttempt(), this::randomFailureAttempt)
            );
            default -> throw new AssertionError("Should never get here");
        };
    }

    private String randomName() {
        return randomAlphaOfLengthBetween(5, 30);
    }

    private DownloadedDatabaseInfo.DownloadAttempt randomSuccessfulAttempt() {
        return randomDownloadAttempt(false);
    }

    private DownloadedDatabaseInfo.DownloadAttempt randomFailureAttempt() {
        return randomDownloadAttempt(true);
    }

    private DownloadedDatabaseInfo.DownloadAttempt randomDownloadAttempt(boolean canHaveErrorMessage) {
        if (randomBoolean()) {
            return null;
        }
        String md5 = randomBoolean() ? null : randomAlphaOfLength(20);
        Long downloadAttemptTimeInMillis = randomBoolean() ? null : randomNonNegativeLong();
        Long downloadDurationInMillis = randomBoolean() ? null : randomNonNegativeLong();
        String source = randomBoolean() ? null : randomAlphaOfLength(20);
        Long buildDateInMillis = randomBoolean() ? null : randomNonNegativeLong();
        String errorMessage = canHaveErrorMessage ? (randomBoolean() ? null : randomAlphaOfLength(2000)) : null;
        return new DownloadedDatabaseInfo.DownloadAttempt(
            md5,
            downloadAttemptTimeInMillis,
            downloadDurationInMillis,
            source,
            buildDateInMillis,
            errorMessage
        );
    }
}
