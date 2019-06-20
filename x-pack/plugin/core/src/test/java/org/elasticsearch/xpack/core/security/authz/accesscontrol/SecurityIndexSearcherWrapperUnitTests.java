/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetReader.DocumentSubsetDirectoryReader;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.security.authz.accesscontrol.SecurityIndexSearcherWrapper.intersectScorerAndRoleBits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityIndexSearcherWrapperUnitTests extends ESTestCase {

    private static final Set<String> META_FIELDS;
    static {
        final Set<String> metaFields = new HashSet<>(Arrays.asList(MapperService.getAllMetaFields()));
        metaFields.add(SourceFieldMapper.NAME);
        metaFields.add(FieldNamesFieldMapper.NAME);
        metaFields.add(SeqNoFieldMapper.NAME);
        META_FIELDS = Collections.unmodifiableSet(metaFields);
    }

    private ThreadContext threadContext;
    private ScriptService scriptService;
    private SecurityIndexSearcherWrapper securityIndexSearcherWrapper;
    private ElasticsearchDirectoryReader esIn;
    private XPackLicenseState licenseState;
    private IndexSettings indexSettings;

    @Before
    public void setup() throws Exception {
        Index index = new Index("_index", "testUUID");
        scriptService = mock(ScriptService.class);
        indexSettings = IndexSettingsModule.newIndexSettings(index, Settings.EMPTY);

        ShardId shardId = new ShardId(index, 0);
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(true);
        threadContext = new ThreadContext(Settings.EMPTY);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);

        Directory directory = new MMapDirectory(createTempDir());
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());
        writer.close();

        DirectoryReader in = DirectoryReader.open(directory); // unfortunately DirectoryReader isn't mock friendly
        esIn = ElasticsearchDirectoryReader.wrap(in, shardId);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        esIn.close();
    }

    public void testDefaultMetaFields() throws Exception {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(null, null, threadContext, licenseState, scriptService) {
            @Override
            protected IndicesAccessControl getIndicesAccessControl() {
                IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(true,
                        new FieldPermissions(fieldPermissionDef(new String[]{}, null)), DocumentPermissions.allowAll());
                return new IndicesAccessControl(true, singletonMap("_index", indexAccessControl));
            }
        };

        FieldSubsetReader.FieldSubsetDirectoryReader result =
                (FieldSubsetReader.FieldSubsetDirectoryReader) securityIndexSearcherWrapper.wrap(esIn);
        assertThat(result.getFilter().run("_uid"), is(true));
        assertThat(result.getFilter().run("_id"), is(true));
        assertThat(result.getFilter().run("_version"), is(true));
        assertThat(result.getFilter().run("_type"), is(true));
        assertThat(result.getFilter().run("_source"), is(true));
        assertThat(result.getFilter().run("_routing"), is(true));
        assertThat(result.getFilter().run("_timestamp"), is(true));
        assertThat(result.getFilter().run("_ttl"), is(true));
        assertThat(result.getFilter().run("_size"), is(true));
        assertThat(result.getFilter().run("_index"), is(true));
        assertThat(result.getFilter().run("_field_names"), is(true));
        assertThat(result.getFilter().run("_seq_no"), is(true));
        assertThat(result.getFilter().run("_some_random_meta_field"), is(true));
        assertThat(result.getFilter().run("some_random_regular_field"), is(false));
    }

    public void testWrapReaderWhenFeatureDisabled() throws Exception {
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(false);
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(null, null, threadContext, licenseState, scriptService);
        DirectoryReader reader = securityIndexSearcherWrapper.wrap(esIn);
        assertThat(reader, sameInstance(esIn));
    }

    public void testWrapSearcherWhenFeatureDisabled() throws Exception {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(null, null, threadContext, licenseState, scriptService);
        IndexSearcher indexSearcher = new IndexSearcher(esIn);
        IndexSearcher result = securityIndexSearcherWrapper.wrap(indexSearcher);
        assertThat(result, sameInstance(indexSearcher));
    }

    public void testWildcards() throws Exception {
        Set<String> expected = new HashSet<>(META_FIELDS);
        expected.add("field1_a");
        expected.add("field1_b");
        expected.add("field1_c");
        assertResolved(new FieldPermissions(fieldPermissionDef(new String[] {"field1*"}, null)), expected, "field", "field2");
    }

    public void testDotNotion() throws Exception {
        Set<String> expected = new HashSet<>(META_FIELDS);
        expected.add("foo.bar");
        assertResolved(new FieldPermissions(fieldPermissionDef(new String[] {"foo.bar"}, null)), expected, "foo", "foo.baz", "bar.foo");

        expected = new HashSet<>(META_FIELDS);
        expected.add("foo.bar");
        assertResolved(new FieldPermissions(fieldPermissionDef(new String[] {"foo.*"}, null)), expected, "foo", "bar");
    }

    public void testDelegateSimilarity() throws Exception {
        IndexSettings settings = IndexSettingsModule.newIndexSettings("_index", Settings.EMPTY);
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(settings, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {
            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        });
        DirectoryReader directoryReader = DocumentSubsetReader.wrap(esIn, bitsetFilterCache, new MatchAllDocsQuery());
        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);
        indexSearcher.setSimilarity(new SweetSpotSimilarity());
        indexSearcher.setQueryCachingPolicy(new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {
            }

            @Override
            public boolean shouldCache(Query query) {
                return false;
            }
        });
        indexSearcher.setQueryCache((weight, policy) -> weight);
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(null, null, threadContext, licenseState, scriptService);
        IndexSearcher result = securityIndexSearcherWrapper.wrap(indexSearcher);
        assertThat(result, not(sameInstance(indexSearcher)));
        assertThat(result.getSimilarity(), sameInstance(indexSearcher.getSimilarity()));
        assertThat(result.getQueryCachingPolicy(), sameInstance(indexSearcher.getQueryCachingPolicy()));
        assertThat(result.getQueryCache(), sameInstance(indexSearcher.getQueryCache()));
        bitsetFilterCache.close();
    }

    public void testIntersectScorerAndRoleBits() throws Exception {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(null, null, threadContext, licenseState, scriptService);
        final Directory directory = newDirectory();
        IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
        );

        Document document = new Document();
        document.add(new StringField("field1", "value1", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value2", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value3", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value4", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        iw.commit();
        iw.deleteDocuments(new Term("field1", "value3"));
        iw.close();
        DirectoryReader directoryReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        Weight weight = searcher.createWeight(new TermQuery(new Term("field2", "value1")),
                org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1f);

        LeafReaderContext leaf = directoryReader.leaves().get(0);

        SparseFixedBitSet sparseFixedBitSet = query(leaf, "field1", "value1");
        LeafCollector leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(0));
            }
        };
        intersectScorerAndRoleBits(weight.scorer(leaf), sparseFixedBitSet, leafCollector, leaf.reader().getLiveDocs());

        sparseFixedBitSet = query(leaf, "field1", "value2");
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(1));
            }
        };
        intersectScorerAndRoleBits(weight.scorer(leaf), sparseFixedBitSet, leafCollector, leaf.reader().getLiveDocs());


        sparseFixedBitSet = query(leaf, "field1", "value3");
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                fail("docId [" + doc + "] should have been deleted");
            }
        };
        intersectScorerAndRoleBits(weight.scorer(leaf), sparseFixedBitSet, leafCollector, leaf.reader().getLiveDocs());

        sparseFixedBitSet = query(leaf, "field1", "value4");
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(3));
            }
        };
        intersectScorerAndRoleBits(weight.scorer(leaf), sparseFixedBitSet, leafCollector, leaf.reader().getLiveDocs());

        directoryReader.close();
        directory.close();
    }

    private void assertResolved(FieldPermissions permissions, Set<String> expected, String... fieldsToTest) {
        for (String field : expected) {
            assertThat(field, permissions.grantsAccessTo(field), is(true));
        }
        for (String field : fieldsToTest) {
            assertThat(field, permissions.grantsAccessTo(field), is(expected.contains(field)));
        }
    }

    public void testFieldPermissionsWithFieldExceptions() throws Exception {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(null, null, threadContext, licenseState, null);
        String[] grantedFields = new String[]{};
        String[] deniedFields;
        Set<String> expected = new HashSet<>(META_FIELDS);
        // Presence of fields in a role with an empty array implies access to no fields except the meta fields
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, randomBoolean() ? null : new String[]{})),
                expected, "foo", "bar");

        // make sure meta fields cannot be denied access to
        deniedFields = META_FIELDS.toArray(new String[0]);
        assertResolved(new FieldPermissions(fieldPermissionDef(null, deniedFields)),
                new HashSet<>(Arrays.asList("foo", "bar", "_some_plugin_meta_field")));

        // check we can add all fields with *
        grantedFields = new String[]{"*"};
        expected = new HashSet<>(META_FIELDS);
        expected.add("foo");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, randomBoolean() ? null : new String[]{})), expected);

        // same with null
        grantedFields = null;
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, randomBoolean() ? null : new String[]{})), expected);

        // check we remove only excluded fields
        grantedFields = new String[]{"*"};
        deniedFields = new String[]{"xfield"};
        expected = new HashSet<>(META_FIELDS);
        expected.add("foo");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "xfield");

        // same with null
        grantedFields = null;
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "xfield");

        // some other checks
        grantedFields = new String[]{"field*"};
        deniedFields = new String[]{"field1", "field2"};
        expected = new HashSet<>(META_FIELDS);
        expected.add("field3");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "field1", "field2");

        grantedFields = new String[]{"field1", "field2"};
        deniedFields = new String[]{"field2"};
        expected = new HashSet<>(META_FIELDS);
        expected.add("field1");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "field1", "field2");

        grantedFields = new String[]{"field*"};
        deniedFields = new String[]{"field2"};
        expected = new HashSet<>(META_FIELDS);
        expected.add("field1");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected, "field2");

        deniedFields = new String[]{"field*"};
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)),
                META_FIELDS, "field1", "field2");

        // empty array for allowed fields always means no field is allowed
        grantedFields = new String[]{};
        deniedFields = new String[]{};
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)),
                META_FIELDS, "field1", "field2");

        // make sure all field can be explicitly allowed
        grantedFields = new String[]{"*"};
        deniedFields = randomBoolean() ? null : new String[]{};
        expected = new HashSet<>(META_FIELDS);
        expected.add("field1");
        assertResolved(new FieldPermissions(fieldPermissionDef(grantedFields, deniedFields)), expected);
    }

    private SparseFixedBitSet query(LeafReaderContext leaf, String field, String value) throws IOException {
        SparseFixedBitSet sparseFixedBitSet = new SparseFixedBitSet(leaf.reader().maxDoc());
        TermsEnum tenum = leaf.reader().terms(field).iterator();
        while (tenum.next().utf8ToString().equals(value) == false) {
        }
        PostingsEnum penum = tenum.postings(null);
        sparseFixedBitSet.or(penum);
        return sparseFixedBitSet;
    }

    public void testIndexSearcherWrapperSparseNoDeletions() throws IOException {
        doTestIndexSearcherWrapper(true, false);
    }

    public void testIndexSearcherWrapperDenseNoDeletions() throws IOException {
        doTestIndexSearcherWrapper(false, false);
    }

    public void testIndexSearcherWrapperSparseWithDeletions() throws IOException {
        doTestIndexSearcherWrapper(true, true);
    }

    public void testIndexSearcherWrapperDenseWithDeletions() throws IOException {
        doTestIndexSearcherWrapper(false, true);
    }

    static class CreateScorerOnceWeight extends Weight {

        private final Weight weight;
        private final Set<Object> seenLeaves = Collections.newSetFromMap(new IdentityHashMap<>());

        protected CreateScorerOnceWeight(Weight weight) {
            super(weight.getQuery());
            this.weight = weight;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            weight.extractTerms(terms);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return weight.explain(context, doc);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            assertTrue(seenLeaves.add(context.reader().getCoreCacheHelper().getKey()));
            return weight.scorer(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context)
                throws IOException {
            assertTrue(seenLeaves.add(context.reader().getCoreCacheHelper().getKey()));
            return weight.bulkScorer(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }

    static class CreateScorerOnceQuery extends Query {

        private final Query query;

        CreateScorerOnceQuery(Query query) {
            this.query = query;
        }

        @Override
        public String toString(String field) {
            return query.toString(field);
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            Query queryRewritten = query.rewrite(reader);
            if (query != queryRewritten) {
                return new CreateScorerOnceQuery(queryRewritten);
            }
            return super.rewrite(reader);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost) throws IOException {
            return new CreateScorerOnceWeight(query.createWeight(searcher, scoreMode, boost));
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && query.equals(((CreateScorerOnceQuery) obj).query);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + query.hashCode();
        }
    }

    public void doTestIndexSearcherWrapper(boolean sparse, boolean deletions) throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(null));
        Document doc = new Document();
        StringField allowedField = new StringField("allowed", "yes", Store.NO);
        doc.add(allowedField);
        StringField fooField = new StringField("foo", "bar", Store.NO);
        doc.add(fooField);
        StringField deleteField = new StringField("delete", "no", Store.NO);
        doc.add(deleteField);
        w.addDocument(doc);
        if (deletions) {
            // add a document that matches foo:bar but will be deleted
            deleteField.setStringValue("yes");
            w.addDocument(doc);
            deleteField.setStringValue("no");
        }
        allowedField.setStringValue("no");
        w.addDocument(doc);
        if (sparse) {
            for (int i = 0; i < 1000; ++i) {
                w.addDocument(doc);
            }
            w.forceMerge(1);
        }
        w.deleteDocuments(new Term("delete", "yes"));

        IndexSettings settings = IndexSettingsModule.newIndexSettings("_index", Settings.EMPTY);
        BitsetFilterCache.Listener listener = new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {

            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        };
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(w), new ShardId(indexSettings.getIndex(), 0));
        BitsetFilterCache cache = new BitsetFilterCache(settings, listener);
        Query roleQuery = new TermQuery(new Term("allowed", "yes"));
        BitSet bitSet = cache.getBitSetProducer(roleQuery).getBitSet(reader.leaves().get(0));
        if (sparse) {
            assertThat(bitSet, instanceOf(SparseFixedBitSet.class));
        } else {
            assertThat(bitSet, instanceOf(FixedBitSet.class));
        }

        DocumentSubsetDirectoryReader filteredReader = DocumentSubsetReader.wrap(reader, cache, roleQuery);
        IndexSearcher searcher = new SecurityIndexSearcherWrapper.IndexSearcherWrapper(filteredReader);

        // Searching a non-existing term will trigger a null scorer
        assertEquals(0, searcher.count(new TermQuery(new Term("non_existing_field", "non_existing_value"))));

        assertEquals(1, searcher.count(new TermQuery(new Term("foo", "bar"))));

        // make sure scorers are created only once, see #1725
        assertEquals(1, searcher.count(new CreateScorerOnceQuery(new MatchAllDocsQuery())));
        IOUtils.close(reader, w, dir);
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }
}
