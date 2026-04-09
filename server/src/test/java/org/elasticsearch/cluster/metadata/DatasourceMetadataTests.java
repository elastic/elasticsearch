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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class DatasourceMetadataTests extends AbstractChunkedSerializingTestCase<DatasourceMetadata> {

    @Override
    protected DatasourceMetadata doParseInstance(XContentParser parser) throws IOException {
        return DatasourceMetadata.fromXContent(parser);
    }

    @Override
    protected DatasourceMetadata createTestInstance() {
        return randomDatasourceMetadata();
    }

    @Override
    protected DatasourceMetadata mutateInstance(DatasourceMetadata instance) {
        Map<String, Datasource> datasources = new HashMap<>(instance.datasources());
        if (datasources.isEmpty()) {
            String name = randomName();
            return new DatasourceMetadata(Map.of(name, randomDatasource(name)));
        }
        datasources.replaceAll((name, datasource) -> randomValueOtherThan(datasource, () -> randomDatasource(name)));
        return new DatasourceMetadata(datasources);
    }

    @Override
    protected Writeable.Reader<DatasourceMetadata> instanceReader() {
        return DatasourceMetadata::readFromStream;
    }

    private static DatasourceMetadata randomDatasourceMetadata() {
        int numDatasources = randomIntBetween(0, 8);
        Map<String, Datasource> datasources = new HashMap<>(numDatasources);
        for (int i = 0; i < numDatasources; i++) {
            String name = randomName();
            datasources.put(name, randomDatasource(name));
        }
        return new DatasourceMetadata(datasources);
    }

    static String randomName() {
        return randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    static Datasource randomDatasource(String name) {
        String type = randomFrom("s3", "gcs", "azure");
        String description = randomBoolean() ? null : randomAlphaOfLengthBetween(0, 32);
        int numSettings = randomIntBetween(0, 4);
        Map<String, DataSourceStoredSetting> settings = new HashMap<>(numSettings);
        for (int i = 0; i < numSettings; i++) {
            settings.put(randomAlphaOfLength(6).toLowerCase(Locale.ROOT), randomStoredSetting());
        }
        return new Datasource(name, type, description, settings);
    }

    static DataSourceStoredSetting randomStoredSetting() {
        Object value = randomFrom(randomAlphaOfLength(8), randomInt(), randomBoolean());
        return new DataSourceStoredSetting(value, randomBoolean());
    }
}
