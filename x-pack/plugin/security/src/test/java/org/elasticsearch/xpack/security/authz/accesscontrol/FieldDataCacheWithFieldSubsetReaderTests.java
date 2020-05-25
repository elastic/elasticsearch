/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

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
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.FieldSubsetReader;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class FieldDataCacheWithFieldSubsetReaderTests extends ESTestCase {

    private SortedSetOrdinalsIndexFieldData sortedSetOrdinalsIndexFieldData;
    private PagedBytesIndexFieldData pagedBytesIndexFieldData;

    private DirectoryReader ir;

    private long numDocs;
    private Directory dir;
    private DummyAccountingFieldDataCache indexFieldDataCache;

    @Before
    public void setup() throws Exception {
        IndexSettings indexSettings = createIndexSettings();
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        String name = "_field";
        indexFieldDataCache = new DummyAccountingFieldDataCache();
        sortedSetOrdinalsIndexFieldData = new SortedSetOrdinalsIndexFieldData(indexSettings,indexFieldDataCache,  name,
                circuitBreakerService, AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
        pagedBytesIndexFieldData = new PagedBytesIndexFieldData(indexSettings, name, indexFieldDataCache,
                circuitBreakerService, TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
                TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
                TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE);

        dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        numDocs = scaledRandomIntBetween(32, 128);

        for (int i = 1; i <= numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("_field", String.valueOf(i), Field.Store.NO));
            doc.add(new SortedSetDocValuesField("_field", new BytesRef(String.valueOf(i))));
            iw.addDocument(doc);
            if (i % 24 == 0) {
                iw.commit();
            }
        }
        iw.close();
        ir = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId(indexSettings.getIndex(), 0));
    }

    @After
    public void destroy() throws Exception {
        ir.close();
        dir.close();
    }

    public void testSortedSetDVOrdinalsIndexFieldData_global() throws Exception {
        assertThat(indexFieldDataCache.topLevelBuilds, equalTo(0));
        IndexOrdinalsFieldData global = sortedSetOrdinalsIndexFieldData.loadGlobal(ir);
        LeafOrdinalsFieldData atomic = global.load(ir.leaves().get(0));
        assertThat(atomic.getOrdinalsValues().getValueCount(), equalTo(numDocs));
        assertThat(indexFieldDataCache.topLevelBuilds, equalTo(1));

        DirectoryReader ir = FieldSubsetReader.wrap(this.ir, new CharacterRunAutomaton(Automata.makeEmpty()));
        global = sortedSetOrdinalsIndexFieldData.loadGlobal(ir);
        atomic = global.load(ir.leaves().get(0));
        assertThat(atomic.getOrdinalsValues().getValueCount(), equalTo(0L));
        assertThat(indexFieldDataCache.topLevelBuilds, equalTo(1));
    }

    public void testSortedSetDVOrdinalsIndexFieldData_segment() throws Exception {
        for (LeafReaderContext context : ir.leaves()) {
            LeafOrdinalsFieldData atomic = sortedSetOrdinalsIndexFieldData.load(context);
            assertThat(atomic.getOrdinalsValues().getValueCount(), greaterThanOrEqualTo(1L));
        }

        DirectoryReader ir = FieldSubsetReader.wrap(this.ir, new CharacterRunAutomaton(Automata.makeEmpty()));
        for (LeafReaderContext context : ir.leaves()) {
            LeafOrdinalsFieldData atomic = sortedSetOrdinalsIndexFieldData.load(context);
            assertThat(atomic.getOrdinalsValues().getValueCount(), equalTo(0L));
        }
        // dv based field data doesn't use index field data cache, so in the end noting should have been added
        assertThat(indexFieldDataCache.leafLevelBuilds, equalTo(0));
    }

    public void testPagedBytesIndexFieldData_global() throws Exception {
        assertThat(indexFieldDataCache.topLevelBuilds, equalTo(0));
        IndexOrdinalsFieldData global = pagedBytesIndexFieldData.loadGlobal(ir);
        LeafOrdinalsFieldData atomic = global.load(ir.leaves().get(0));
        assertThat(atomic.getOrdinalsValues().getValueCount(), equalTo(numDocs));
        assertThat(indexFieldDataCache.topLevelBuilds, equalTo(1));

        DirectoryReader ir = FieldSubsetReader.wrap(this.ir, new CharacterRunAutomaton(Automata.makeEmpty()));
        global = pagedBytesIndexFieldData.loadGlobal(ir);
        atomic = global.load(ir.leaves().get(0));
        assertThat(atomic.getOrdinalsValues().getValueCount(), equalTo(0L));
        assertThat(indexFieldDataCache.topLevelBuilds, equalTo(1));
    }

    public void testPagedBytesIndexFieldData_segment() throws Exception {
        assertThat(indexFieldDataCache.leafLevelBuilds, equalTo(0));
        for (LeafReaderContext context : ir.leaves()) {
            LeafOrdinalsFieldData atomic = pagedBytesIndexFieldData.load(context);
            assertThat(atomic.getOrdinalsValues().getValueCount(), greaterThanOrEqualTo(1L));
        }
        assertThat(indexFieldDataCache.leafLevelBuilds, equalTo(ir.leaves().size()));

        DirectoryReader ir = FieldSubsetReader.wrap(this.ir, new CharacterRunAutomaton(Automata.makeEmpty()));
        for (LeafReaderContext context : ir.leaves()) {
            LeafOrdinalsFieldData atomic = pagedBytesIndexFieldData.load(context);
            assertThat(atomic.getOrdinalsValues().getValueCount(), equalTo(0L));
        }
        assertThat(indexFieldDataCache.leafLevelBuilds, equalTo(ir.leaves().size()));
    }

    private IndexSettings createIndexSettings() {
        Settings settings = Settings.EMPTY;
        IndexMetadata indexMetadata = IndexMetadata.builder("_name")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build();
        return new IndexSettings(indexMetadata, settings);
    }

    private static class DummyAccountingFieldDataCache implements IndexFieldDataCache {

        private int leafLevelBuilds = 0;
        private int topLevelBuilds = 0;

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
                throws Exception {
            leafLevelBuilds++;
            return indexFieldData.loadDirect(context);
        }

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader,
                                                                                          IFD indexFieldData) throws Exception {
            topLevelBuilds++;
            return (IFD) indexFieldData.localGlobalDirect(indexReader);
        }

        @Override
        public void clear() {
        }

        @Override
        public void clear(String fieldName) {
        }

    }

}
