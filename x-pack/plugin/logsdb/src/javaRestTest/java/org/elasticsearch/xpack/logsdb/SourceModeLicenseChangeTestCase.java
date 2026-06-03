/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.function.BooleanSupplier;

public abstract class SourceModeLicenseChangeTestCase extends DataStreamLicenseChangeTestCase {

    protected interface TestCase {
        String dataStreamName();

        void prepareDataStream() throws IOException;

        String indexMode();

        SourceFieldMapper.Mode initialMode();

        SourceFieldMapper.Mode finalMode();

        void rollover() throws IOException;

        /** Return {@code true} to skip this case entirely (e.g. when a required feature flag is not enabled on the cluster). */
        default boolean skip() {
            return false;
        }
    }

    protected record SourceModeTestCase(
        String dataStreamName,
        String indexMode,
        SourceFieldMapper.Mode initialMode,
        SourceFieldMapper.Mode finalMode,
        BooleanSupplier shouldSkip
    ) implements TestCase {
        public SourceModeTestCase(
            String dataStreamName,
            String indexMode,
            SourceFieldMapper.Mode initialMode,
            SourceFieldMapper.Mode finalMode
        ) {
            this(dataStreamName, indexMode, initialMode, finalMode, () -> false);
        }

        @Override
        public void prepareDataStream() throws IOException {
            var template = """
                {
                  "index_patterns": ["%ds_name%"],
                  "priority": 100,
                  "data_stream": {},
                  "template": {
                    "settings": {
                      "index": {
                        "mode": "%index_mode%"
                      }
                    }
                  }
                }
                """.replace("%ds_name%", dataStreamName).replace("%index_mode%", indexMode);
            putTemplate(client(), dataStreamName + "-template", template);
            assertOK(createDataStream(client(), dataStreamName()));
        }

        @Override
        public void rollover() throws IOException {
            rolloverDataStream(client(), dataStreamName());
        }

        @Override
        public boolean skip() {
            return shouldSkip.getAsBoolean();
        }
    }

    protected abstract void licenseChange() throws IOException;

    protected abstract void applyInitialLicense() throws IOException;

    protected abstract List<TestCase> cases();

    public void testLicenseChange() throws IOException {
        applyInitialLicense();

        for (var testCase : cases()) {
            if (testCase.skip()) {
                continue;
            }
            testCase.prepareDataStream();

            var indexMode = (String) getSetting(client(), getDataStreamBackingIndex(client(), testCase.dataStreamName(), 0), "index.mode");
            assertEquals(testCase.indexMode(), indexMode);

            var sourceMode = (String) getSetting(
                client(),
                getDataStreamBackingIndex(client(), testCase.dataStreamName(), 0),
                "index.mapping.source.mode"
            );
            assertEquals(testCase.initialMode().toString(), sourceMode);
        }

        licenseChange();

        for (var testCase : cases()) {
            if (testCase.skip()) {
                continue;
            }
            testCase.rollover();

            var indexMode = (String) getSetting(client(), getDataStreamBackingIndex(client(), testCase.dataStreamName(), 1), "index.mode");
            assertEquals(testCase.indexMode(), indexMode);

            var sourceMode = (String) getSetting(
                client(),
                getDataStreamBackingIndex(client(), testCase.dataStreamName(), 1),
                "index.mapping.source.mode"
            );
            assertEquals(testCase.finalMode().toString(), sourceMode);
        }
    }

    protected static Response removeComponentTemplate(final RestClient client, final String componentTemplate) throws IOException {
        final Request request = new Request("DELETE", "/_component_template/" + componentTemplate);
        return client.performRequest(request);
    }
}
