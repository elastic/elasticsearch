/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.datastreams.DataStreamSettingsIT.putComposableIndexTemplate;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamMigrationToTimeSeriesIT extends ESIntegTestCase {

    private static final String TEMPLATE = "index_template_test-ds";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testMigrationToTimeSeriesThatShouldFail() throws IOException {
        IndexMode indexMode = randomValueOtherThanMany(
            mode -> mode == IndexMode.STANDARD || mode == IndexMode.TIME_SERIES,
            () -> randomFrom(IndexMode.values())
        );
        doTestMigrationToTimeSeries(indexMode, false);
    }

    public void testMigrationToTimeSeriesShouldSucceed() throws IOException {
        doTestMigrationToTimeSeries(IndexMode.STANDARD, true);
    }

    private void doTestMigrationToTimeSeries(IndexMode initialMode, boolean supported) throws IOException {
        String dataStreamName = "from-" + initialMode.getName() + "-to-tsdb";
        putComposableIndexTemplate(
            TEMPLATE,
            List.of("from-*-to-tsdb"),
            Settings.builder().put(IndexSettings.MODE.getKey(), initialMode).build()
        );
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
            )
        );
        waitForDataStreamBackingIndices(dataStreamName, 1);

        putComposableIndexTemplate(
            TEMPLATE,
            List.of("from-*-to-tsdb"),
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "test")
                .build()
        );
        if (supported) {
            RolloverResponse rolloverResponse = indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet();
            assertThat(rolloverResponse.isRolledOver(), equalTo(true));
            waitForDataStreamBackingIndices(dataStreamName, 2);
        } else {
            Exception exception = expectThrows(
                Exception.class,
                () -> indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet()
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "to [time_series] is not supported, please first change the [index.mode] "
                        + "in the template to [standard], roll over, and try again."
                )
            );
        }
    }
}
