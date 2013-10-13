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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;

// we might wanna cut this over to LuceneTestCase
public abstract class AbstractFieldDataTests extends ElasticsearchTestCase {

    protected IndexFieldDataService ifdService;
    protected IndexWriter writer;
    protected AtomicReaderContext readerContext;

    protected abstract FieldDataType getFieldDataType();

    public <IFD extends IndexFieldData> IFD getForField(String fieldName) {
        return ifdService.getForField(new FieldMapper.Names(fieldName), getFieldDataType());
    }

    @Before
    public void setup() throws Exception {
        ifdService = new IndexFieldDataService(new Index("test"));
        // LogByteSizeMP to preserve doc ID order
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)).setMergePolicy(new LogByteSizeMergePolicy()));
    }

    protected AtomicReaderContext refreshReader() throws Exception {
        if (readerContext != null) {
            readerContext.reader().close();
        }
        AtomicReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(writer, true));
        readerContext = reader.getContext();
        return readerContext;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (readerContext != null) {
            readerContext.reader().close();
        }
        writer.close();
        ifdService.clear();
    }

}
