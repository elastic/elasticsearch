/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Arrays;
import java.util.List;

public class StatusInfoTests extends ESTestCase {
    public void testSerialization() throws Exception {
        StatusInfo statusInfo = new StatusInfo(randomFrom(StatusInfo.Status.values()), randomAlphaOfLengthBetween(0, 200));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            statusInfo,
            history -> copyWriteable(statusInfo, writableRegistry(), StatusInfo::new),
            this::mutateStatusInfo
        );
    }

    private StatusInfo mutateStatusInfo(StatusInfo originalStatusInfo) {
        switch (randomIntBetween(1, 2)) {
            case 1 -> {
                List<StatusInfo.Status> unusedStatuses = Arrays.stream(StatusInfo.Status.values())
                    .filter(status -> status.equals(originalStatusInfo.status()) == false)
                    .toList();
                return new StatusInfo(randomFrom(unusedStatuses), originalStatusInfo.info());
            }
            case 2 -> {
                return new StatusInfo(originalStatusInfo.status(), randomAlphaOfLength(100));
            }
            default -> throw new IllegalStateException();
        }
    }
}
