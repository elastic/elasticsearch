/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.net.ConnectException;
import java.util.EnumSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class CsvTestsDataLoaderTests extends ESTestCase {

    /**
     * Types that cannot be stored as a user field. A new {@link DataType} fails this test until
     * {@link UnionTypeCsvDatasets} classifies it.
     */
    private static final Set<DataType> NOT_AN_INDEX_FIELD = EnumSet.of(
        DataType.UNSUPPORTED,
        DataType.NULL,
        DataType.SOURCE,
        DataType.DATE_PERIOD,
        DataType.TIME_DURATION,
        DataType.DOC_DATA_TYPE,
        DataType.TSID_DATA_TYPE,
        DataType.PARTIAL_AGG,
        DataType.GEOHASH,
        DataType.GEOTILE,
        DataType.GEOHEX
    );

    public void testCsvTestsDataLoaderExecution() {
        ConnectException ce = expectThrows(ConnectException.class, () -> CsvTestsDataLoader.main(new String[] {}));
        assertThat(ce.getMessage(), startsWith("Connection refused"));
    }

    public void testUnionTypeDatasetsCoverIndexableDataTypes() throws IOException {
        for (DataType type : DataType.values()) {
            CsvTestsDataLoader.TestDataset dataset = CsvTestsDataLoader.CSV_DATASET.get(
                UnionTypeCsvDatasets.INDEX_PREFIX + type.typeName()
            );
            if (NOT_AN_INDEX_FIELD.contains(type)) {
                assertNull("no index for " + type, dataset);
                continue;
            }
            assertNotNull("missing union index for " + type, dataset);
            assertThat(dataset.loadMappings(), containsString("\"field\""));
            assertThat(dataset.loadMappings(), containsString("\"type\": \"keyword\""));
            String document = CsvTestsDataLoader.readCsvDocuments(dataset.streamData(), dataset.allowSubFields()).get(0).json().toString();
            assertThat(document, containsString("\"type\":\"" + type.typeName() + "\""));
            assertThat(document, containsString("\"field\":"));
        }
    }
}
