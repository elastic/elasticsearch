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

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public abstract class EngineTestCase extends ESTestCase {

    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 1);
    protected static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    protected ThreadPool threadPool;

    protected Store store;
    protected Store storeReplica;

    protected InternalEngine engine;
    protected InternalEngine replicaEngine;

    protected IndexSettings defaultSettings;
    protected String codecName;
    protected Path primaryTranslogDir;
    protected Path replicaTranslogDir;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        CodecService codecService = new CodecService(null, logger);
        String name = Codec.getDefault().getName();
        if (Arrays.asList(codecService.availableCodecs()).contains(name)) {
            // some codecs are read only so we only take the ones that we have in the service and randomly
            // selected by lucene test case.
            codecName = name;
        } else {
            codecName = "default";
        }
        defaultSettings = IndexSettingsModule.newIndexSettings("test", Settings.builder()
                .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
                .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                        between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY)))
                .build()); // TODO randomize more settings
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
        storeReplica = createStore();
        Lucene.cleanLuceneIndex(store.directory());
        Lucene.cleanLuceneIndex(storeReplica.directory());
        primaryTranslogDir = createTempDir("translog-primary");
        engine = createEngine(store, primaryTranslogDir);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
        replicaTranslogDir = createTempDir("translog-replica");
        replicaEngine = createEngine(storeReplica, replicaTranslogDir);
        currentIndexWriterConfig = replicaEngine.getCurrentIndexWriterConfig();

        assertEquals(replicaEngine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
    }

    public EngineConfig copy(EngineConfig config, EngineConfig.OpenMode openMode) {
        return copy(config, openMode, config.getAnalyzer());
    }

    public EngineConfig copy(EngineConfig config, EngineConfig.OpenMode openMode, Analyzer analyzer) {
        return new EngineConfig(openMode, config.getShardId(), config.getThreadPool(), config.getIndexSettings(), config.getWarmer(),
                config.getStore(), config.getDeletionPolicy(), config.getMergePolicy(), analyzer, config.getSimilarity(),
                new CodecService(null, logger), config.getEventListener(), config.getTranslogRecoveryPerformer(), config.getQueryCache(),
                config.getQueryCachingPolicy(), config.getTranslogConfig(), config.getFlushMergesAfter(), config.getRefreshListeners(),
                config.getMaxUnsafeAutoIdTimestamp());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(
                replicaEngine, storeReplica,
                engine, store);
        terminate(threadPool);
    }


    protected static ParseContext.Document testDocumentWithTextField() {
        return testDocumentWithTextField("test");
    }

    protected static ParseContext.Document testDocumentWithTextField(String value) {
        ParseContext.Document document = testDocument();
        document.add(new TextField("value", value, Field.Store.YES));
        return document;
    }


    protected static ParseContext.Document testDocument() {
        return new ParseContext.Document();
    }

    public static ParsedDocument createParsedDoc(String id, String type, String routing) {
        return testParsedDocument(id, type, routing, -1, -1, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"), null);
    }

    protected static ParsedDocument testParsedDocument(
            String id,
            String type,
            String routing,
            long timestamp,
            long ttl,
            ParseContext.Document document,
            BytesReference source,
            Mapping mappingUpdate) {
        Field uidField = new Field("_uid", Uid.createUid(type, id), UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        document.add(uidField);
        document.add(versionField);
        BytesRef ref = source.toBytesRef();
        document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        return new ParsedDocument(versionField, id, type, routing, timestamp, ttl, Arrays.asList(document), source, XContentType.JSON,
                mappingUpdate);
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        final DirectoryService directoryService = new DirectoryService(shardId, INDEX_SETTINGS) {
            @Override
            public Directory newDirectory() throws IOException {
                return directory;
            }

            @Override
            public long throttleTimeInNanos() {
                return 0;
            }
        };
        return new Store(shardId, INDEX_SETTINGS, directoryService, new DummyShardLock(shardId));
    }

    protected Translog createTranslog() throws IOException {
        return createTranslog(primaryTranslogDir);
    }

    protected Translog createTranslog(Path translogPath) throws IOException {
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE);
        return new Translog(translogConfig, null);
    }

    protected SnapshotDeletionPolicy createSnapshotDeletionPolicy() {
        return new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    }

    protected InternalEngine createEngine(Store store, Path translogPath) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy());
    }

    protected InternalEngine createEngine(
            IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, null);
    }

    protected InternalEngine createEngine(
            IndexSettings indexSettings,
            Store store,
            Path translogPath,
            MergePolicy mergePolicy,
            @Nullable IndexWriterFactory indexWriterFactory) throws IOException {
        EngineConfig config = config(indexSettings, store, translogPath, mergePolicy, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null);
        InternalEngine internalEngine = new InternalEngine(config) {
            @Override
            IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                return (indexWriterFactory != null) ? indexWriterFactory.createWriter(directory, iwc) : super.createWriter(directory, iwc);
            }

        };
        if (config.getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            internalEngine.recoverFromTranslog();
        }
        return internalEngine;
    }

    @FunctionalInterface
    public interface IndexWriterFactory {
        IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException;
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               long maxUnsafeAutoIdTimestamp, ReferenceManager.RefreshListener refreshListener) {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        final EngineConfig.OpenMode openMode;
        try {
            if (Lucene.indexExists(store.directory()) == false) {
                openMode = EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG;
            } else {
                openMode = EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
            }
        } catch (IOException e) {
            throw new ElasticsearchException("can't find index?", e);
        }
        Engine.EventListener listener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
                // we don't need to notify anybody in this test
            }
        };
        final List<ReferenceManager.RefreshListener> refreshListenerList =
                refreshListener == null ? emptyList() : Collections.singletonList(refreshListener);
        EngineConfig config = new EngineConfig(openMode, shardId, threadPool, indexSettings, null, store, createSnapshotDeletionPolicy(),
                mergePolicy, iwc.getAnalyzer(), iwc.getSimilarity(), new CodecService(null, logger), listener,
                new TranslogHandler(xContentRegistry(), shardId.getIndexName(), logger), IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(), translogConfig, TimeValue.timeValueMinutes(5), refreshListenerList,
                maxUnsafeAutoIdTimestamp);

        return config;
    }

    public static final BytesReference B_1 = new BytesArray(new byte[]{1});
    public static final BytesReference B_2 = new BytesArray(new byte[]{2});
    public static final BytesReference B_3 = new BytesArray(new byte[]{3});
    public static final BytesArray SOURCE = bytesArray("{}");

    protected static BytesArray bytesArray(String string) {
        return new BytesArray(string.getBytes(Charset.defaultCharset()));
    }

    protected Term newUid(String id) {
        return new Term("_uid", id);
    }

    protected Term newUid(ParsedDocument doc) {
        return new Term("_uid", Uid.createUid(doc.type(), doc.id()));
    }

    protected Engine.Index indexForDoc(ParsedDocument doc) {
        return new Engine.Index(newUid(doc), doc);
    }

    protected Engine.Index replicaIndexForDoc(ParsedDocument doc, long version, boolean isRetry) {
        return  new Engine.Index(newUid(doc), doc, version,  VersionType.EXTERNAL,
                Engine.Operation.Origin.REPLICA, System.nanoTime(),
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, isRetry);
    }

}
