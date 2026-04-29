/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormatTests;
import org.elasticsearch.index.mapper.IdFieldMapper;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormatTests.runTestWithRandomDocs;
import static org.hamcrest.Matchers.equalTo;

public class TSDBStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        var tsdbStoredFieldsFormat = new TSDBStoredFieldsFormat(new Lucene90StoredFieldsFormat());
        return new AssertingCodec() {
            @Override
            public StoredFieldsFormat storedFieldsFormat() {
                return tsdbStoredFieldsFormat;
            }
        };
    }

    public void testSyntheticId() throws Exception {
        runTestWithRandomDocs((writer, finalDocs) -> {
            try (var reader = DirectoryReader.open(writer)) {
                final var storedFields = reader.storedFields();

                int totalDocs = 0;
                for (var leafReaderContext : reader.leaves()) {
                    totalDocs += leafReaderContext.reader().maxDoc();
                }
                assertThat(totalDocs, equalTo(finalDocs.values().stream().mapToInt(TSDBSyntheticIdPostingsFormatTests.Doc::version).sum()));

                int docID = 0;
                final var expectedIds = finalDocs.navigableKeySet().iterator();
                while (expectedIds.hasNext()) {
                    final var expectedId = expectedIds.next();
                    for (int version = 0; version < finalDocs.get(expectedId).version(); version++) {
                        var document = storedFields.document(docID);
                        assertThat(document.getField(IdFieldMapper.NAME).binaryValue(), equalTo(expectedId));
                        docID += 1;
                    }
                }
            }
        });
    }
}
