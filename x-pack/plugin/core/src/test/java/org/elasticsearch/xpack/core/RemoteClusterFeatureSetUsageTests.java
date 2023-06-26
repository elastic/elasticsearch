/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ProxyConnectionStrategy;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.SniffConnectionStrategy;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterFeatureSetUsageTests extends ESTestCase {

    public void testToXContent() throws IOException {
        final int numberOfRemoteClusters = randomIntBetween(0, 10);
        int numberOfSniffModes = 0;
        int numberOfApiKeySecured = 0;
        final List<RemoteConnectionInfo> infos = new ArrayList<>();
        for (int i = 0; i < numberOfRemoteClusters; i++) {
            final boolean hasCredentials = randomBoolean();
            if (hasCredentials) {
                numberOfApiKeySecured += 1;
            }
            final RemoteConnectionInfo.ModeInfo modeInfo;
            if (randomBoolean()) {
                modeInfo = new SniffConnectionStrategy.SniffModeInfo(List.of(), randomIntBetween(1, 8), randomIntBetween(1, 8));
                numberOfSniffModes += 1;
            } else {
                modeInfo = new ProxyConnectionStrategy.ProxyModeInfo(
                    randomAlphaOfLengthBetween(3, 8),
                    randomAlphaOfLengthBetween(3, 8),
                    randomIntBetween(1, 8),
                    randomIntBetween(1, 8)
                );
            }
            infos.add(
                new RemoteConnectionInfo(randomAlphaOfLengthBetween(3, 8), modeInfo, TimeValue.ZERO, randomBoolean(), hasCredentials)
            );
        }

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            new RemoteClusterFeatureSetUsage(infos).toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(
                Strings.toString(builder),
                equalTo(
                    XContentHelper.stripWhitespace(
                        Strings.format(
                            """
                                {
                                  "size": %s,
                                  "mode": {
                                    "proxy": %s,
                                    "sniff": %s
                                  },
                                  "security": {
                                     "cert": %s,
                                     "api_key": %s
                                  }
                                }""",
                            numberOfRemoteClusters,
                            numberOfRemoteClusters - numberOfSniffModes,
                            numberOfSniffModes,
                            numberOfRemoteClusters - numberOfApiKeySecured,
                            numberOfApiKeySecured
                        )
                    )
                )
            );
        }

    }
}
