/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.tests.index.ForceMergePolicy;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class StoredFieldCodecDuelTests extends ESTestCase {

    private static final String FIELD_1 = "string_field_1";
    private static final String FIELD_2 = "binary_field_2";
    private static final String FIELD_3 = "int_field_3";
    private static final String FIELD_4 = "long_field_4";
    private static final String FIELD_5 = "float_field_5";
    private static final String FIELD_6 = "double_field_5";

    public void testDuel() throws IOException {
        try (var baselineDirectory = newDirectory(); var contenderDirectory = newDirectory()) {
            int numDocs = randomIntBetween(256, 8096);

            var mergePolicy = new ForceMergePolicy(newLogMergePolicy());
            var baselineConfig = newIndexWriterConfig();
            baselineConfig.setMergePolicy(mergePolicy);
            baselineConfig.setCodec(
                new LegacyPerFieldMapperCodec(Lucene99Codec.Mode.BEST_COMPRESSION, null, BigArrays.NON_RECYCLING_INSTANCE)
            );
            var contenderConf = newIndexWriterConfig();
            contenderConf.setCodec(
                new PerFieldMapperCodec(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION, null, BigArrays.NON_RECYCLING_INSTANCE)
            );
            contenderConf.setMergePolicy(mergePolicy);

            try (
                var baselineIw = new RandomIndexWriter(random(), baselineDirectory, baselineConfig);
                var contenderIw = new RandomIndexWriter(random(), contenderDirectory, contenderConf)
            ) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new StoredField(FIELD_1, randomAlphaOfLength(randomIntBetween(1, 4096))));
                    doc.add(new StoredField(FIELD_2, randomByteArrayOfLength(randomIntBetween(1, 4096))));
                    doc.add(new StoredField(FIELD_3, randomInt()));
                    doc.add(new StoredField(FIELD_4, randomLong()));
                    doc.add(new StoredField(FIELD_5, randomFloat()));
                    doc.add(new StoredField(FIELD_6, randomDouble()));
                    baselineIw.addDocument(doc);
                    contenderIw.addDocument(doc);
                }
                baselineIw.forceMerge(1);
                contenderIw.forceMerge(1);
            }
            try (var baselineIr = DirectoryReader.open(baselineDirectory); var contenderIr = DirectoryReader.open(contenderDirectory)) {
                assertEquals(1, baselineIr.leaves().size());
                assertEquals(1, contenderIr.leaves().size());

                var baseLeafReader = baselineIr.leaves().get(0).reader();
                var contenderLeafReader = contenderIr.leaves().get(0).reader();
                assertEquals(baseLeafReader.maxDoc(), contenderLeafReader.maxDoc());

                for (int docId = 0; docId < contenderLeafReader.maxDoc(); docId++) {
                    Document baselineDoc = baseLeafReader.storedFields().document(docId);
                    Document contenderDoc = contenderLeafReader.storedFields().document(docId);
                    assertThat(contenderDoc.getFields().size(), equalTo(baselineDoc.getFields().size()));
                    for (int i = 0; i < baselineDoc.getFields().size(); i++) {
                        var baselineField = baselineDoc.getFields().get(i);
                        var contenderField = contenderDoc.getFields().get(i);
                        assertThat(contenderField.name(), equalTo(baselineField.name()));
                        switch (baselineField.name()) {
                            case FIELD_1 -> assertThat(contenderField.stringValue(), equalTo(baselineField.stringValue()));
                            case FIELD_2 -> assertThat(contenderField.binaryValue(), equalTo(baselineField.binaryValue()));
                            case FIELD_3, FIELD_4, FIELD_5, FIELD_6 -> assertThat(
                                contenderField.numericValue(),
                                equalTo(baselineField.numericValue())
                            );
                            default -> fail("unexpected field [" + baselineField.name() + "]");
                        }
                    }
                }
            }
        }
    }

}
