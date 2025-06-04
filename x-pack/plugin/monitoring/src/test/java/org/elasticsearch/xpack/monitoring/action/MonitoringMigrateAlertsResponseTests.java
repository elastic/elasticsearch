/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse.ExporterMigrationResult;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MonitoringMigrateAlertsResponseTests extends AbstractWireSerializingTestCase<MonitoringMigrateAlertsResponse> {

    private static final class TestException extends IOException {
        TestException(String message) {
            super(message);
        }
    }

    private final List<Exception> exceptionSamples = new ArrayList<>();

    @Before
    public void populateExceptionSamples() {
        // A lineup of exceptions to make sure a number of errors are able to be serialized, even if they are not equal to themselves on
        // the other side of the wire.
        exceptionSamples.add(new ElasticsearchException(randomAlphaOfLength(15))); // Eminently serializable
        exceptionSamples.add(new ElasticsearchTimeoutException(randomAlphaOfLength(15))); // Inherited serialization
        exceptionSamples.add(new IOException(randomAlphaOfLength(15))); // Well known, special handling
        exceptionSamples.add(new TestException(randomAlphaOfLength(15))); // Class resolution loss during serialization
        exceptionSamples.add(new RuntimeException(randomAlphaOfLength(15))); // Unserializable
    }

    @Override
    protected Writeable.Reader<MonitoringMigrateAlertsResponse> instanceReader() {
        return MonitoringMigrateAlertsResponse::new;
    }

    @Override
    protected MonitoringMigrateAlertsResponse createTestInstance() {
        List<ExporterMigrationResult> results = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            String name = randomAlphaOfLength(10);
            String type = randomFrom(LocalExporter.TYPE, HttpExporter.TYPE);
            boolean migrationComplete = randomBoolean();
            Exception reason = null;
            if (migrationComplete == false) {
                reason = randomFrom(exceptionSamples);
            }
            ExporterMigrationResult result = new ExporterMigrationResult(name, type, migrationComplete, reason);
            results.add(result);
        }
        return new MonitoringMigrateAlertsResponse(results);
    }

    @Override
    protected MonitoringMigrateAlertsResponse mutateInstance(MonitoringMigrateAlertsResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
