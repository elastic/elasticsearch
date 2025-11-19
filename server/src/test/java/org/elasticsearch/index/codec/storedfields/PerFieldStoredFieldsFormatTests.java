/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PerFieldStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        var bloomFilterSizeInKb = atLeast(1);
        var bloomFilterStoredFieldsFormat = new ES93BloomFilterStoredFieldsFormat(
            BigArrays.NON_RECYCLING_INSTANCE,
            ByteSizeValue.ofKb(bloomFilterSizeInKb),
            IdFieldMapper.NAME
        );
        var defaultStoredFields = new ESLucene90StoredFieldsFormat();

        return new AssertingCodec() {
            @Override
            public StoredFieldsFormat storedFieldsFormat() {
                return new PerFieldStoredFieldsFormat() {
                    @Override
                    protected ESStoredFieldsFormat getStoredFieldsFormatForField(String field) {
                        if (field.equals(IdFieldMapper.NAME)) {
                            return bloomFilterStoredFieldsFormat;
                        }
                        return defaultStoredFields;
                    }
                };
            }
        };
    }

    public void testConflictingFileExtensionsThrowAnException() throws Exception {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            var bloomFilterStoredFieldsFormat = new ES93BloomFilterStoredFieldsFormat(
                BigArrays.NON_RECYCLING_INSTANCE,
                ByteSizeValue.ofKb(1),
                IdFieldMapper.NAME
            );

            var defaultStoredFields = new ESLucene90StoredFieldsFormat() {
                @Override
                protected Set<String> getFileExtensions() {
                    return Set.of(ES93BloomFilterStoredFieldsFormat.STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION);
                }
            };

            conf.setCodec(new AssertingCodec() {
                @Override
                public StoredFieldsFormat storedFieldsFormat() {
                    return new PerFieldStoredFieldsFormat() {
                        @Override
                        protected ESStoredFieldsFormat getStoredFieldsFormatForField(String field) {
                            if (field.equals(IdFieldMapper.NAME)) {
                                return bloomFilterStoredFieldsFormat;
                            }
                            return defaultStoredFields;
                        }
                    };
                }
            });
            conf.setMergePolicy(newLogMergePolicy());
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                Document doc = new Document();
                var id = UUIDs.randomBase64UUID();
                doc.add(new StringField(IdFieldMapper.NAME, new BytesRef(id.getBytes(StandardCharsets.UTF_8)), Field.Store.YES));
                doc.add(new StringField("host", "host", Field.Store.YES));
                doc.add(new LongField("counter", 1, Field.Store.YES));
                var exception = expectThrows(IllegalStateException.class, () -> writer.addDocument(doc));
                assertThat(
                    exception.getMessage(),
                    equalTo(
                        "File extension conflict for field 'host': format ESLucene90StoredFieldsFormat "
                            + "has overlapping fileExtensions with existing format"
                    )
                );
            }
        }
    }
}
