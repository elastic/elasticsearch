/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DesiredNodeSerializationTests extends AbstractSerializingTestCase<DesiredNode> {
    @Override
    protected DesiredNode doParseInstance(XContentParser parser) throws IOException {
        return DesiredNode.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DesiredNode> instanceReader() {
        return DesiredNode::new;
    }

    @Override
    protected DesiredNode createTestInstance() {
        return randomDesiredNode();
    }

    public static DesiredNode randomDesiredNode() {
        return new DesiredNode(
            randomSetting(),
            randomAlphaOfLength(20),
            randomIntBetween(1, 256),
            ByteSizeValue.ofGb(randomIntBetween(1, 1024)),
            ByteSizeValue.ofTb(randomIntBetween(1, 40)),
            Version.CURRENT
        );
    }

    private static Settings randomSetting() {
        int numSettings = randomIntBetween(0, 20);
        Settings.Builder settingsBuilder = Settings.builder();

        for (int i = 0; i < numSettings; i++) {
            putRandomSetting(settingsBuilder);
        }
        return settingsBuilder.build();
    }

    private static void putRandomSetting(Settings.Builder settings) {
        final String key = randomAlphaOfLength(10);
        switch (randomIntBetween(0, 7)) {
            case 0 -> settings.put(key, randomAlphaOfLength(20));
            case 1 -> settings.put(key, randomInt());
            case 2 -> settings.put(key, randomLong());
            case 3 -> settings.put(key, randomFloat());
            case 4 -> settings.put(key, randomDouble());
            case 5 -> settings.put(key, TimeValue.timeValueMillis(randomIntBetween(1, 1000)));
            case 6 -> settings.put(key, ByteSizeValue.ofGb(randomIntBetween(1, 20)));
            case 7 -> settings.putList(key, randomList(1, 10, () -> randomAlphaOfLength(20)));
            default -> throw new IllegalArgumentException();
        }
    }
}
