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

public class DataSourceMetadataTests extends AbstractChunkedSerializingTestCase<DataSourceMetadata> {

    @Override
    protected DataSourceMetadata doParseInstance(XContentParser parser) throws IOException {
        return DataSourceMetadata.fromXContent(parser);
    }

    @Override
    protected DataSourceMetadata createTestInstance() {
        return randomDataSourceMetadata();
    }

    @Override
    protected DataSourceMetadata mutateInstance(DataSourceMetadata instance) {
        Map<String, DataSource> dataSources = new HashMap<>(instance.dataSources());
        if (dataSources.isEmpty()) {
            String name = randomName();
            return new DataSourceMetadata(Map.of(name, randomDataSource(name)));
        }
        dataSources.replaceAll((name, dataSource) -> randomValueOtherThan(dataSource, () -> randomDataSource(name)));
        return new DataSourceMetadata(dataSources);
    }

    @Override
    protected Writeable.Reader<DataSourceMetadata> instanceReader() {
        return DataSourceMetadata::readFromStream;
    }

    private static DataSourceMetadata randomDataSourceMetadata() {
        int numDataSources = randomIntBetween(0, 8);
        Map<String, DataSource> dataSources = new HashMap<>(numDataSources);
        for (int i = 0; i < numDataSources; i++) {
            String name = randomName();
            dataSources.put(name, randomDataSource(name));
        }
        return new DataSourceMetadata(dataSources);
    }

    static String randomName() {
        return randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    static DataSource randomDataSource(String name) {
        String type = randomFrom("s3", "gcs", "azure");
        String description = randomBoolean() ? null : randomAlphaOfLengthBetween(0, 32);
        int numSettings = randomIntBetween(0, 4);
        Map<String, DataSourceSetting> settings = new HashMap<>(numSettings);
        for (int i = 0; i < numSettings; i++) {
            settings.put(randomAlphaOfLength(6).toLowerCase(Locale.ROOT), randomStoredSetting());
        }
        return new DataSource(name, type, description, settings);
    }

    static DataSourceSetting randomStoredSetting() {
        boolean secret = randomBoolean();
        // Secret settings must be String-valued; non-secret may carry any JSON-native type.
        Object value = secret ? randomAlphaOfLength(8) : randomFrom(randomAlphaOfLength(8), randomInt(), randomBoolean());
        return new DataSourceSetting(value, secret);
    }

    public void testContextIsGatewayOnly() {
        // Regression guard. The cluster-state framework reads context() to decide inclusion:
        // - API is excluded because the raw XContent contains plaintext secret setting values, which must not appear
        // in GET /_cluster/state.
        // - SNAPSHOT is excluded because snapshot restore has no mechanism to re-provision secrets; restoring a
        // cluster state containing data sources would produce unusable configurations.
        DataSourceMetadata metadata = new DataSourceMetadata(
            Map.of("my-s3", new DataSource("my-s3", "s3", null, Map.of("access_key", new DataSourceSetting("AKIA_LEAK_CHECK", true))))
        );
        assertEquals(EnumSet.of(Metadata.XContentContext.GATEWAY), metadata.context());
        assertFalse(metadata.context().contains(Metadata.XContentContext.API));
        assertFalse(metadata.context().contains(Metadata.XContentContext.SNAPSHOT));
    }
}
