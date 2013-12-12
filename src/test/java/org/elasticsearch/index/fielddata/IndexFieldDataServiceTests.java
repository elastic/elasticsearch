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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.plain.*;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

public class IndexFieldDataServiceTests extends ElasticsearchTestCase {

    public void testChangeFieldDataFormat() throws Exception {
        final IndexFieldDataService ifdService = new IndexFieldDataService(new Index("test"));
        final BuilderContext ctx = new BuilderContext(null, new ContentPath(1));
        final StringFieldMapper mapper1 = MapperBuilders.stringField("s").tokenized(false).fieldDataSettings(ImmutableSettings.builder().put(FieldDataType.FORMAT_KEY, "paged_bytes").build()).build(ctx);
        final IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(TEST_VERSION_CURRENT, new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("s", "thisisastring", Store.NO));
        writer.addDocument(doc);
        final IndexReader reader1 = DirectoryReader.open(writer, true);
        IndexFieldData<?> ifd = ifdService.getForField(mapper1);
        assertThat(ifd, instanceOf(PagedBytesIndexFieldData.class));
        Set<AtomicReader> oldSegments = Collections.newSetFromMap(new IdentityHashMap<AtomicReader, Boolean>());
        for (AtomicReaderContext arc : reader1.leaves()) {
            oldSegments.add(arc.reader());
            AtomicFieldData<?> afd = ifd.load(arc);
            assertThat(afd, instanceOf(PagedBytesAtomicFieldData.class));
        }
        // write new segment
        writer.addDocument(doc);
        final IndexReader reader2 = DirectoryReader.open(writer, true);
        final StringFieldMapper mapper2 = MapperBuilders.stringField("s").tokenized(false).fieldDataSettings(ImmutableSettings.builder().put(FieldDataType.FORMAT_KEY, "fst").build()).build(ctx);
        ifdService.onMappingUpdate();
        ifd = ifdService.getForField(mapper2);
        assertThat(ifd, instanceOf(FSTBytesIndexFieldData.class));
        for (AtomicReaderContext arc : reader2.leaves()) {
            AtomicFieldData<?> afd = ifd.load(arc);
            if (oldSegments.contains(arc.reader())) {
                assertThat(afd, instanceOf(PagedBytesAtomicFieldData.class));
            } else {
                assertThat(afd, instanceOf(FSTBytesAtomicFieldData.class));
            }
        }
        reader1.close();
        reader2.close();
        writer.close();
        writer.getDirectory().close();
    }

}
