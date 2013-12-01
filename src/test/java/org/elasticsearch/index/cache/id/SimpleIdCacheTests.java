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

package org.elasticsearch.index.cache.id;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.id.simple.SimpleIdCache;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class SimpleIdCacheTests extends ElasticsearchTestCase {

    @Test
    public void testDeletedDocuments() throws Exception {
        SimpleIdCache idCache = createSimpleIdCache(Tuple.tuple("child", "parent"));
        IndexWriter writer = createIndexWriter();
        // Begins with parent, ends with child docs
        final Document parent = doc("parent", "1");
        writer.addDocument(parent);
        writer.addDocument(childDoc("child", "1", "parent", "1"));
        writer.addDocument(childDoc("child", "2", "parent", "1"));
        writer.addDocument(childDoc("child", "3", "parent", "1"));
        writer.commit();

        final String parentUid = parent.get("_uid");
        assert parentUid != null;
        writer.deleteDocuments(new Term("_uid", parentUid));

        writer.close();
        DirectoryReader topLevelReader = DirectoryReader.open(writer.getDirectory());
        List<AtomicReaderContext> leaves = topLevelReader.getContext().leaves();
        idCache.refresh(leaves);

        assertThat(leaves.size(), equalTo(1));
        IdReaderCache readerCache = idCache.reader(leaves.get(0).reader());
        IdReaderTypeCache typeCache = readerCache.type("parent");
        assertThat(typeCache.idByDoc(0).toUtf8(), equalTo("1"));
    }

    @Test
    public void testRefresh() throws Exception {
        SimpleIdCache idCache = createSimpleIdCache(Tuple.tuple("child", "parent"));
        IndexWriter writer = createIndexWriter();
        // Begins with parent, ends with child docs
        writer.addDocument(doc("parent", "1"));
        writer.addDocument(childDoc("child", "1", "parent", "1"));
        writer.addDocument(childDoc("child", "2", "parent", "1"));
        writer.addDocument(childDoc("child", "3", "parent", "1"));
        writer.addDocument(childDoc("child", "4", "parent", "1"));
        writer.commit();

        // Begins with child, ends with parent docs
        writer.addDocument(childDoc("child", "5", "parent", "2"));
        writer.addDocument(doc("parent", "2"));
        writer.addDocument(doc("parent", "3"));
        writer.addDocument(doc("parent", "4"));
        writer.addDocument(doc("parent", "5"));
        writer.commit();

        // Begins with parent, child docs in the middle and ends with parent doc
        writer.addDocument(doc("parent", "6"));
        writer.addDocument(childDoc("child", "6", "parent", "6"));
        writer.addDocument(childDoc("child", "7", "parent", "6"));
        writer.addDocument(childDoc("child", "8", "parent", "5"));
        writer.addDocument(childDoc("child", "9", "parent", "4"));
        writer.addDocument(doc("parent", "7"));
        writer.commit();

        // Garbage segment
        writer.addDocument(doc("zzz", "1"));
        writer.addDocument(doc("xxx", "2"));
        writer.addDocument(doc("aaa", "3"));
        writer.addDocument(doc("ccc", "4"));
        writer.addDocument(doc("parent", "8"));
        writer.commit();

        writer.close();
        DirectoryReader topLevelReader = DirectoryReader.open(writer.getDirectory());
        List<AtomicReaderContext> leaves = topLevelReader.getContext().leaves();
        idCache.refresh(leaves);

        // Verify simple id cache for segment 1
        IdReaderCache readerCache = idCache.reader(leaves.get(0).reader());
        assertThat(readerCache.type("child"), nullValue());
        IdReaderTypeCache typeCache = readerCache.type("parent");
        assertThat(typeCache.idByDoc(0).toUtf8(), equalTo("1"));
        assertThat(typeCache.idByDoc(1), nullValue());
        assertThat(typeCache.idByDoc(2), nullValue());
        assertThat(typeCache.idByDoc(3), nullValue());
        assertThat(typeCache.idByDoc(4), nullValue());

        assertThat(typeCache.parentIdByDoc(0), nullValue());
        assertThat(typeCache.parentIdByDoc(1).toUtf8(), equalTo("1"));
        assertThat(typeCache.parentIdByDoc(2).toUtf8(), equalTo("1"));
        assertThat(typeCache.parentIdByDoc(3).toUtf8(), equalTo("1"));
        assertThat(typeCache.parentIdByDoc(4).toUtf8(), equalTo("1"));

        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("1"))), equalTo(0));
        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("2"))), equalTo(-1));
        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("3"))), equalTo(-1));
        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("4"))), equalTo(-1));

        // Verify simple id cache for segment 2
        readerCache = idCache.reader(leaves.get(1).reader());
        assertThat(readerCache.type("child"), nullValue());
        typeCache = readerCache.type("parent");
        assertThat(typeCache.idByDoc(0), nullValue());
        assertThat(typeCache.idByDoc(1).toUtf8(), equalTo("2"));
        assertThat(typeCache.idByDoc(2).toUtf8(), equalTo("3"));
        assertThat(typeCache.idByDoc(3).toUtf8(), equalTo("4"));
        assertThat(typeCache.idByDoc(4).toUtf8(), equalTo("5"));

        assertThat(typeCache.parentIdByDoc(0).toUtf8(), equalTo("2"));
        assertThat(typeCache.parentIdByDoc(1), nullValue());
        assertThat(typeCache.parentIdByDoc(2), nullValue());
        assertThat(typeCache.parentIdByDoc(3), nullValue());
        assertThat(typeCache.parentIdByDoc(4), nullValue());

        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("2"))), equalTo(1));
        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("3"))), equalTo(2));
        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("4"))), equalTo(3));
        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("5"))), equalTo(4));

        // Verify simple id cache for segment 3
        readerCache = idCache.reader(leaves.get(2).reader());
        assertThat(readerCache.type("child"), nullValue());
        typeCache = readerCache.type("parent");
        assertThat(typeCache.idByDoc(0).toUtf8(), equalTo("6"));
        assertThat(typeCache.idByDoc(1), nullValue());
        assertThat(typeCache.idByDoc(2), nullValue());
        assertThat(typeCache.idByDoc(3), nullValue());
        assertThat(typeCache.idByDoc(4), nullValue());
        assertThat(typeCache.idByDoc(5).toUtf8(), equalTo("7"));

        assertThat(typeCache.parentIdByDoc(0), nullValue());
        assertThat(typeCache.parentIdByDoc(1).toUtf8(), equalTo("6"));
        assertThat(typeCache.parentIdByDoc(2).toUtf8(), equalTo("6"));
        assertThat(typeCache.parentIdByDoc(3).toUtf8(), equalTo("5"));
        assertThat(typeCache.parentIdByDoc(4).toUtf8(), equalTo("4"));
        assertThat(typeCache.parentIdByDoc(5), nullValue());

        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("6"))), equalTo(0));
        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("7"))), equalTo(5));

        // Verify simple id cache for segment 4
        readerCache = idCache.reader(leaves.get(3).reader());
        assertThat(readerCache.type("child"), nullValue());
        typeCache = readerCache.type("parent");
        assertThat(typeCache.idByDoc(0), nullValue());
        assertThat(typeCache.idByDoc(1), nullValue());
        assertThat(typeCache.idByDoc(2), nullValue());
        assertThat(typeCache.idByDoc(3), nullValue());
        assertThat(typeCache.idByDoc(4).toUtf8(), equalTo("8"));

        assertThat(typeCache.parentIdByDoc(0), nullValue());
        assertThat(typeCache.parentIdByDoc(1), nullValue());
        assertThat(typeCache.parentIdByDoc(2), nullValue());
        assertThat(typeCache.parentIdByDoc(3), nullValue());
        assertThat(typeCache.parentIdByDoc(4), nullValue());

        assertThat(typeCache.docById(new HashedBytesArray(Strings.toUTF8Bytes("8"))), equalTo(4));
    }

    @Test(expected = AssertionError.class)
    public void testRefresh_tripAssert() throws Exception {
        SimpleIdCache idCache = createSimpleIdCache(Tuple.tuple("child", "parent"));
        IndexWriter writer = createIndexWriter();
        // Begins with parent, ends with child docs
        writer.addDocument(doc("parent", "1"));
        writer.addDocument(childDoc("child", "1", "parent", "1"));
        writer.addDocument(childDoc("child", "2", "parent", "1"));
        writer.addDocument(childDoc("child", "3", "parent", "1"));
        writer.addDocument(childDoc("child", "4", "parent", "1"));
        // Doc like this should never end up in the index, just wanna trip an assert here!
        Document document = new Document();
        document.add(new StringField(UidFieldMapper.NAME, "parent", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        writer.close();
        DirectoryReader topLevelReader = DirectoryReader.open(writer.getDirectory());
        List<AtomicReaderContext> leaves = topLevelReader.getContext().leaves();
        idCache.refresh(leaves);
    }

    private Document doc(String type, String id) {
        Document parent = new Document();
        parent.add(new StringField(UidFieldMapper.NAME, String.format(Locale.ROOT, "%s#%s", type, id), Field.Store.NO));
        return parent;
    }

    private Document childDoc(String type, String id, String parentType, String parentId) {
        Document parent = new Document();
        parent.add(new StringField(UidFieldMapper.NAME, String.format(Locale.ROOT, "%s#%s", type, id), Field.Store.NO));
        parent.add(new StringField(ParentFieldMapper.NAME, String.format(Locale.ROOT, "%s#%s", parentType, parentId), Field.Store.NO));
        return parent;
    }

    private SimpleIdCache createSimpleIdCache(Tuple<String, String>... documentTypes) throws IOException {
        Settings settings = ImmutableSettings.EMPTY;
        Index index = new Index("test");
        SimpleIdCache idCache = new SimpleIdCache(index, settings);
        MapperService mapperService = MapperTestUtils.newMapperService();

        for (Tuple<String, String> documentType : documentTypes) {
            String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(documentType.v1())
                    .startObject("_parent").field("type", documentType.v2()).endObject()
                    .endObject().endObject().string();
            mapperService.merge(documentType.v1(), new CompressedString(defaultMapping), true);
        }

        idCache.setIndexService(new StubIndexService(mapperService, null));
        return idCache;
    }

    private IndexWriter createIndexWriter() throws IOException {
        return new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)));
    }

    public static class StubIndexService implements IndexService {

        private final MapperService mapperService;
        private final IndexAliasesService aliasesService;

        public StubIndexService(MapperService mapperService, IndexAliasesService aliasesService) {
            this.mapperService = mapperService;
            this.aliasesService = aliasesService;
        }

        @Override
        public Injector injector() {
            return null;
        }

        @Override
        public IndexGateway gateway() {
            return null;
        }

        @Override
        public IndexCache cache() {
            return null;
        }

        @Override
        public IndexFieldDataService fieldData() {
            return null;
        }

        @Override
        public IndexSettingsService settingsService() {
            return null;
        }

        @Override
        public PercolatorService percolateService() {
            return null;
        }

        @Override
        public AnalysisService analysisService() {
            return null;
        }

        @Override
        public MapperService mapperService() {
            return mapperService;
        }

        @Override
        public IndexQueryParserService queryParserService() {
            return null;
        }

        @Override
        public SimilarityService similarityService() {
            return null;
        }

        @Override
        public IndexAliasesService aliasesService() {
            return aliasesService;
        }

        @Override
        public IndexEngine engine() {
            return null;
        }

        @Override
        public IndexStore store() {
            return null;
        }

        @Override
        public IndexShard createShard(int sShardId) throws ElasticSearchException {
            return null;
        }

        @Override
        public void removeShard(int shardId, String reason) throws ElasticSearchException {
        }

        @Override
        public int numberOfShards() {
            return 0;
        }

        @Override
        public ImmutableSet<Integer> shardIds() {
            return null;
        }

        @Override
        public boolean hasShard(int shardId) {
            return false;
        }

        @Override
        public IndexShard shard(int shardId) {
            return null;
        }

        @Override
        public IndexShard shardSafe(int shardId) throws IndexShardMissingException {
            return null;
        }

        @Override
        public Injector shardInjector(int shardId) {
            return null;
        }

        @Override
        public Injector shardInjectorSafe(int shardId) throws IndexShardMissingException {
            return null;
        }

        @Override
        public String indexUUID() {
            return IndexMetaData.INDEX_UUID_NA_VALUE;
        }

        @Override
        public Index index() {
            return null;
        }

        @Override
        public Iterator<IndexShard> iterator() {
            return null;
        }
    }

}
