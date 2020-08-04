/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.FieldMaskingReader;

import static org.hamcrest.Matchers.equalTo;

public class FieldDataCacheTests extends ESTestCase {

    public void testLoadGlobal_neverCacheIfFieldIsMissing() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        long numDocs = scaledRandomIntBetween(32, 128);

        for (int i = 1; i <= numDocs; i++) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field1", new BytesRef(String.valueOf(i))));
            doc.add(new StringField("field2", String.valueOf(i), Field.Store.NO));
            iw.addDocument(doc);
            if (i % 24 == 0) {
                iw.commit();
            }
        }
        iw.close();
        DirectoryReader ir =
            ElasticsearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId("_index", "_na_", 0));

        DummyAccountingFieldDataCache fieldDataCache = new DummyAccountingFieldDataCache();
        // Testing SortedSetOrdinalsIndexFieldData:
        SortedSetOrdinalsIndexFieldData sortedSetOrdinalsIndexFieldData = createSortedDV("field1", fieldDataCache);
        sortedSetOrdinalsIndexFieldData.loadGlobal(ir);
        assertThat(fieldDataCache.cachedGlobally, equalTo(1));
        sortedSetOrdinalsIndexFieldData.loadGlobal(new FieldMaskingReader("field1", ir));
        assertThat(fieldDataCache.cachedGlobally, equalTo(1));

        // Testing PagedBytesIndexFieldData
        PagedBytesIndexFieldData pagedBytesIndexFieldData = createPagedBytes("field2", fieldDataCache);
        pagedBytesIndexFieldData.loadGlobal(ir);
        assertThat(fieldDataCache.cachedGlobally, equalTo(2));
        pagedBytesIndexFieldData.loadGlobal(new FieldMaskingReader("field2", ir));
        assertThat(fieldDataCache.cachedGlobally, equalTo(2));

        ir.close();
        dir.close();
    }

    private SortedSetOrdinalsIndexFieldData createSortedDV(String fieldName, IndexFieldDataCache indexFieldDataCache) {
        return new SortedSetOrdinalsIndexFieldData(indexFieldDataCache, fieldName, CoreValuesSourceType.BYTES,
            new NoneCircuitBreakerService(), AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
    }

    private PagedBytesIndexFieldData createPagedBytes(String fieldName, IndexFieldDataCache indexFieldDataCache) {
        return new PagedBytesIndexFieldData(
            fieldName,
            CoreValuesSourceType.BYTES,
            indexFieldDataCache,
            new NoneCircuitBreakerService(),
            TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
            TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
            TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE
        );
    }

    private class DummyAccountingFieldDataCache implements IndexFieldDataCache {

        private int cachedGlobally = 0;

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
            throws Exception {
            return indexFieldData.loadDirect(context);
        }

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader,
                                                                                          IFD indexFieldData) throws Exception {
            cachedGlobally++;
            return (IFD) indexFieldData.loadGlobalDirect(indexReader);
        }

        @Override
        public void clear() {
        }

        @Override
        public void clear(String fieldName) {
        }
    }

}
