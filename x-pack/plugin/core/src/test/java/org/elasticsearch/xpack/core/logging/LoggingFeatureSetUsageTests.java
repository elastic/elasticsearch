/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.logging;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.EsqlLoggingConfig;
import org.elasticsearch.xpack.core.logging.LoggingFeatureSetUsage.LoggingConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LoggingFeatureSetUsageTests extends AbstractWireSerializingTestCase<LoggingFeatureSetUsage> {

    @Override
    protected Writeable.Reader<LoggingFeatureSetUsage> instanceReader() {
        return LoggingFeatureSetUsage::new;
    }

    @Override
    protected LoggingFeatureSetUsage createTestInstance() {
        return new LoggingFeatureSetUsage(randomEsqlLoggingConfig());
    }

    @Override
    protected LoggingFeatureSetUsage mutateInstance(LoggingFeatureSetUsage instance) throws IOException {
        var esqlConfig = instance.esqlConfig();
        return switch (between(4, 6)) {
            case 4 -> {
                var base = esqlConfig.base();
                var newBase = new LoggingConfig(!base.enabled(), base.userInfo());
                yield new LoggingFeatureSetUsage(new EsqlLoggingConfig(newBase, esqlConfig.thresholds()));
            }
            case 5 -> {
                var base = esqlConfig.base();
                var newBase = new LoggingConfig(base.enabled(), !base.userInfo());
                yield new LoggingFeatureSetUsage(new EsqlLoggingConfig(newBase, esqlConfig.thresholds()));
            }
            case 6 -> {
                Map<String, String> thresholds = new HashMap<>(esqlConfig.thresholds());
                if (thresholds.isEmpty() || randomBoolean()) {
                    thresholds.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
                } else {
                    String key = randomFrom(thresholds.keySet());
                    thresholds.put(key, randomValueOtherThan(thresholds.get(key), () -> randomAlphaOfLength(8)));
                }
                yield new LoggingFeatureSetUsage(new EsqlLoggingConfig(esqlConfig.base(), thresholds));
            }
            default -> throw new AssertionError("unexpected branch");
        };
    }

    private LoggingConfig randomLoggingConfig() {
        return new LoggingConfig(randomBoolean(), randomBoolean());
    }

    private EsqlLoggingConfig randomEsqlLoggingConfig() {
        Map<String, String> thresholds = randomMap(0, 5, () -> Tuple.tuple(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        return new EsqlLoggingConfig(randomLoggingConfig(), thresholds);
    }

    public void testGetMinimalSupportedVersion() {
        assertThat(createTestInstance().getMinimalSupportedVersion(), equalTo(TransportVersion.fromName("logging_xpack_usage")));
    }
}
