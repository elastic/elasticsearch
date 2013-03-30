/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.unit.index.fielddata;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 */
@Test
public abstract class AbstractFieldDataTests {

    protected IndexFieldDataService ifdService;
    protected IndexWriter writer;
    protected AtomicReaderContext readerContext;

    protected abstract FieldDataType getFieldDataType();

    public <IFD extends IndexFieldData> IFD getForField(String fieldName) {
        return ifdService.getForField(new FieldMapper.Names(fieldName), getFieldDataType());
    }

    @BeforeMethod
    public void setup() throws Exception {
        ifdService = new IndexFieldDataService(new Index("test"));
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)));
    }

    protected AtomicReaderContext refreshReader() throws Exception {
        if (readerContext != null) {
            readerContext.reader().close();
        }
        AtomicReader reader = new SlowCompositeReaderWrapper(DirectoryReader.open(writer, true));
        readerContext = reader.getContext();
        return readerContext;
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (readerContext != null) {
            readerContext.reader().close();
        }
        writer.close();
        ifdService.clear();
    }

}
