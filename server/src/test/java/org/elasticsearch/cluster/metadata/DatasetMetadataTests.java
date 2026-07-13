/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class DatasetMetadataTests extends AbstractChunkedSerializingTestCase<DatasetMetadata> {

    @Override
    protected DatasetMetadata doParseInstance(XContentParser parser) throws IOException {
        return DatasetMetadata.fromXContent(parser);
    }

    @Override
    protected DatasetMetadata createTestInstance() {
        return randomDatasetMetadata();
    }

    @Override
    protected DatasetMetadata mutateInstance(DatasetMetadata instance) {
        Map<String, Dataset> datasets = new HashMap<>(instance.datasets());
        if (datasets.isEmpty()) {
            String name = randomName();
            return new DatasetMetadata(Map.of(name, randomDataset(name)));
        }
        datasets.replaceAll((name, dataset) -> randomValueOtherThan(dataset, () -> randomDataset(name)));
        return new DatasetMetadata(datasets);
    }

    @Override
    protected Writeable.Reader<DatasetMetadata> instanceReader() {
        return DatasetMetadata::readFromStream;
    }

    private static DatasetMetadata randomDatasetMetadata() {
        int numDatasets = randomIntBetween(0, 8);
        Map<String, Dataset> datasets = new HashMap<>(numDatasets);
        for (int i = 0; i < numDatasets; i++) {
            String name = randomName();
            datasets.put(name, randomDataset(name));
        }
        return new DatasetMetadata(datasets);
    }

    public void testContextExcludesSnapshot() {
        // Regression guard. Datasets carry no secrets, so API exposure is intentional. SNAPSHOT is excluded to stay
        // consistent with DataSourceMetadata: restoring datasets without their data sources would leave dangling
        // references, so both types must move together when snapshot support is enabled in a future milestone.
        DatasetMetadata metadata = new DatasetMetadata(
            Map.of("my-dataset", new Dataset("my-dataset", new DataSourceReference("my-source"), "s3://bucket/key", null, Map.of()))
        );
        assertEquals(EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY), metadata.context());
        assertFalse(metadata.context().contains(Metadata.XContentContext.SNAPSHOT));
    }

    static String randomName() {
        return randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    static Dataset randomDataset(String name) {
        String dataSource = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        String resource = "s3://" + randomAlphaOfLength(8) + "/" + randomAlphaOfLength(6) + ".parquet";
        String description = randomBoolean() ? null : randomAlphaOfLengthBetween(0, 32);
        int numSettings = randomIntBetween(0, 4);
        Map<String, Object> settings = new HashMap<>(numSettings);
        for (int i = 0; i < numSettings; i++) {
            settings.put(randomAlphaOfLength(6).toLowerCase(Locale.ROOT), randomFrom(randomAlphaOfLength(8), randomInt(), randomBoolean()));
        }
        return new Dataset(name, new DataSourceReference(dataSource), resource, description, settings);
    }
}
