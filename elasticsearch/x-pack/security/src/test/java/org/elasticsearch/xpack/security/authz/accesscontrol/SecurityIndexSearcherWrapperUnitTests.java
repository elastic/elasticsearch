/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

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
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.HasChildQueryBuilder;
import org.elasticsearch.index.query.HasParentQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.xpack.security.authz.accesscontrol.DocumentSubsetReader.DocumentSubsetDirectoryReader;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissions;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.security.authz.accesscontrol.SecurityIndexSearcherWrapper.intersectScorerAndRoleBits;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecurityIndexSearcherWrapperUnitTests extends ESTestCase {

    private ThreadContext threadContext;
    private MapperService mapperService;
    private ScriptService scriptService;
    private SecurityIndexSearcherWrapper securityIndexSearcherWrapper;
    private ElasticsearchDirectoryReader esIn;
    private XPackLicenseState licenseState;
    private IndexSettings indexSettings;

    @Before
    public void before() throws Exception {
        Index index = new Index("_index", "testUUID");
        scriptService = mock(ScriptService.class);
        indexSettings = IndexSettingsModule.newIndexSettings(index, Settings.EMPTY);
        NamedAnalyzer namedAnalyzer = new NamedAnalyzer("default", new StandardAnalyzer());
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(indexSettings, namedAnalyzer, namedAnalyzer, namedAnalyzer,
                Collections.emptyMap());
        SimilarityService similarityService = new SimilarityService(indexSettings, Collections.emptyMap());
        mapperService = new MapperService(indexSettings, indexAnalyzers, similarityService,
                new IndicesModule(emptyList()).getMapperRegistry(), () -> null);

        ShardId shardId = new ShardId(index, 0);
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(true);
        threadContext = new ThreadContext(Settings.EMPTY);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);

        Directory directory = new RAMDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());
        writer.close();

        DirectoryReader in = DirectoryReader.open(directory); // unfortunately DirectoryReader isn't mock friendly
        esIn = ElasticsearchDirectoryReader.wrap(in, shardId);
    }

    @After
    public void after() throws Exception {
        esIn.close();
    }

    public void testDefaultMetaFields() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .endObject()
                .endObject().endObject();
        mapperService.merge("type", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);

        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService) {
            @Override
            protected IndicesAccessControl getIndicesAccessControl() {
                IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(true,
                        new FieldPermissions(new String[]{}, null), null);
                return new IndicesAccessControl(true, singletonMap("_index", indexAccessControl));
            }
        };

        FieldSubsetReader.FieldSubsetDirectoryReader result =
                (FieldSubsetReader.FieldSubsetDirectoryReader) securityIndexSearcherWrapper.wrap(esIn);
        assertThat(result.getFieldNames().size(), equalTo(12));
        assertThat(result.getFieldNames().contains("_uid"), is(true));
        assertThat(result.getFieldNames().contains("_id"), is(true));
        assertThat(result.getFieldNames().contains("_version"), is(true));
        assertThat(result.getFieldNames().contains("_type"), is(true));
        assertThat(result.getFieldNames().contains("_source"), is(true));
        assertThat(result.getFieldNames().contains("_routing"), is(true));
        assertThat(result.getFieldNames().contains("_parent"), is(true));
        assertThat(result.getFieldNames().contains("_timestamp"), is(true));
        assertThat(result.getFieldNames().contains("_ttl"), is(true));
        assertThat(result.getFieldNames().contains("_size"), is(true));
        assertThat(result.getFieldNames().contains("_index"), is(true));
        assertThat(result.getFieldNames().contains("_field_names"), is(true));
        // _all contains actual user data and therefor can't be included by default
        assertThat(result.getFieldNames().contains("_all"), is(false));
    }

    public void testWrapReaderWhenFeatureDisabled() throws Exception {
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(false);
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService);
        DirectoryReader reader = securityIndexSearcherWrapper.wrap(esIn);
        assertThat(reader, sameInstance(esIn));
    }

    public void testWrapSearcherWhenFeatureDisabled() throws Exception {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService);
        IndexSearcher indexSearcher = new IndexSearcher(esIn);
        IndexSearcher result = securityIndexSearcherWrapper.wrap(indexSearcher);
        assertThat(result, sameInstance(indexSearcher));
    }

    public void testWildcards() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field1_a").field("type", "text").endObject()
                .startObject("field1_b").field("type", "text").endObject()
                .startObject("field1_c").field("type", "text").endObject()
                .startObject("field2_a").field("type", "text").endObject()
                .startObject("field2_b").field("type", "text").endObject()
                .startObject("field2_c").field("type", "text").endObject()
                .endObject().endObject().endObject();
        mapperService.merge("type", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertResolvedFields("field1*", "field1_a", "field1_b", "field1_c");
        assertResolvedFields("field2*", "field2_a", "field2_b", "field2_c");
    }

    public void testDotNotion() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo")
                .field("type", "object")
                .startObject("properties")
                .startObject("bar").field("type", "text").endObject()
                .startObject("baz").field("type", "text").endObject()
                .endObject()
                .endObject()
                .startObject("bar")
                .field("type", "object")
                .startObject("properties")
                .startObject("foo").field("type", "text").endObject()
                .startObject("baz").field("type", "text").endObject()
                .endObject()
                .endObject()
                .startObject("baz")
                .field("type", "object")
                .startObject("properties")
                .startObject("bar").field("type", "text").endObject()
                .startObject("foo").field("type", "text").endObject()
                .endObject()
                .endObject()
                .endObject().endObject().endObject();
        mapperService.merge("type", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertResolvedFields("foo.bar", "foo.bar");
        assertResolvedFields("bar.baz", "bar.baz");
        assertResolvedFields("foo.*", "foo.bar", "foo.baz");
        assertResolvedFields("baz.*", "baz.bar", "baz.foo");
    }

    public void testParentChild() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("parent1")
                .startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .endObject()
                .endObject().endObject();
        mapperService.merge("parent1", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mappingSource = jsonBuilder().startObject().startObject("child1")
                .startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .endObject()
                .startObject("_parent")
                .field("type", "parent1")
                .endObject()
                .endObject().endObject();
        mapperService.merge("child1", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mappingSource = jsonBuilder().startObject().startObject("child2")
                .startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .endObject()
                .startObject("_parent")
                .field("type", "parent1")
                .endObject()
                .endObject().endObject();
        mapperService.merge("child2", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mappingSource = jsonBuilder().startObject().startObject("parent2")
                .startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .endObject()
                .endObject().endObject();
        mapperService.merge("parent2", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mappingSource = jsonBuilder().startObject().startObject("child3")
                .startObject("properties")
                .startObject("field").field("type", "text").endObject()
                .endObject()
                .startObject("_parent")
                .field("type", "parent2")
                .endObject()
                .endObject().endObject();
        mapperService.merge("child3", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertResolvedFields("field", "field", ParentFieldMapper.joinField("parent1"), ParentFieldMapper.joinField("parent2"));
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
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService);
        IndexSearcher result = securityIndexSearcherWrapper.wrap(indexSearcher);
        assertThat(result, not(sameInstance(indexSearcher)));
        assertThat(result.getSimilarity(true), sameInstance(indexSearcher.getSimilarity(true)));
        bitsetFilterCache.close();
    }

    public void testIntersectScorerAndRoleBits() throws Exception {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService);
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
        Weight weight = searcher.createNormalizedWeight(new TermQuery(new Term("field2", "value1")), false);

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

    public void testFieldPermissionsWithFieldExceptions() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("some_type")
                .startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .startObject("field2").field("type", "text").endObject()
                .startObject("xfield3").field("type", "text").endObject()
                .endObject()
                .endObject().endObject();
        mapperService.merge("some_type", new CompressedXContent(mappingSource.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, null);
        Set<String> allowedMetaFields = securityIndexSearcherWrapper.getAllowedMetaFields();
        String[] grantedFields = new String[]{};
        String[] deniedFields;
        // Presence of fields in a role with an empty array implies access to no fields except the meta fields
        Set<String> resolvedAllowedFields = new FieldPermissions(grantedFields, randomBoolean() ? null : new String[]{})
                .resolveAllowedFields(allowedMetaFields, mapperService);
        Set<String> expectedResultSet = new HashSet<>(allowedMetaFields);
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // make sure meta fields cannot be denied access to
        deniedFields = allowedMetaFields.toArray(new String[allowedMetaFields.size()]);
        resolvedAllowedFields = new FieldPermissions(null, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field1", "field2", "xfield3"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // check we can add all fields with *
        grantedFields = new String[]{"*"};
        resolvedAllowedFields = new FieldPermissions(grantedFields, randomBoolean() ? null : new String[]{})
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field1", "field2", "xfield3"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // same with null
        resolvedAllowedFields = new FieldPermissions(grantedFields, randomBoolean() ? null : new String[]{})
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field1", "field2", "xfield3"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // check we remove only excluded fields
        grantedFields = new String[]{"*"};
        deniedFields = new String[]{"xfield3"};
        resolvedAllowedFields = new FieldPermissions(grantedFields, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field1", "field2"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // same with null
        deniedFields = new String[]{"field1"};
        resolvedAllowedFields = new FieldPermissions(null, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field2", "xfield3"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // some other checks
        grantedFields = new String[]{"field*"};
        deniedFields = new String[]{"field1", "field2"};
        resolvedAllowedFields = new FieldPermissions(grantedFields, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        grantedFields = new String[]{"field1", "field2"};
        deniedFields = new String[]{"field2"};
        resolvedAllowedFields = new FieldPermissions(grantedFields, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field1"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        grantedFields = new String[]{"field*"};
        deniedFields = new String[]{"field2"};
        resolvedAllowedFields = new FieldPermissions(grantedFields, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field1"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        deniedFields = new String[]{"field*"};
        resolvedAllowedFields = new FieldPermissions(null, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("xfield3"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // empty array for allowed fields always means no field is allowed
        grantedFields = new String[]{};
        deniedFields = new String[]{};
        resolvedAllowedFields = new FieldPermissions(grantedFields, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // make sure all field can be explicitly allowed
        grantedFields = new String[]{"_all", "*"};
        deniedFields = randomBoolean() ? null : new String[]{};
        resolvedAllowedFields = new FieldPermissions(grantedFields, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("field1", "field2", "xfield3", "_all"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));

        // make sure all field can be explicitly allowed
        grantedFields = new String[]{"_all"};
        deniedFields = randomBoolean() ? null : new String[]{};
        resolvedAllowedFields = new FieldPermissions(grantedFields, deniedFields)
                .resolveAllowedFields(allowedMetaFields, mapperService);
        expectedResultSet = new HashSet<>(allowedMetaFields);
        expectedResultSet.addAll(Arrays.asList("_all"));
        assertThat(resolvedAllowedFields.size(), equalTo(expectedResultSet.size()));
        assertThat(resolvedAllowedFields, containsInAnyOrder(expectedResultSet.toArray()));
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

    private void assertResolvedFields(String expression, String... expectedFields) {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService) {
            @Override
            protected IndicesAccessControl getIndicesAccessControl() {
                IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(true,
                        new FieldPermissions(new String[]{expression}, null), null);
                return new IndicesAccessControl(true, singletonMap("_index", indexAccessControl));
            }
        };
        FieldSubsetReader.FieldSubsetDirectoryReader result =
                (FieldSubsetReader.FieldSubsetDirectoryReader) securityIndexSearcherWrapper.wrap(esIn);

        assertThat(result.getFieldNames().size() - securityIndexSearcherWrapper.getAllowedMetaFields().size(),
                equalTo(expectedFields.length));
        for (String expectedField : expectedFields) {
            assertThat(result.getFieldNames().contains(expectedField), is(true));
        }
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

    public void testTemplating() throws Exception {
        User user = new User("_username", new String[]{"role1", "role2"}, "_full_name", "_email",
                Collections.singletonMap("key", "value"), true);
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService) {

                    @Override
                    protected User getUser() {
                        return user;
                    }
                };

        ExecutableScript executableScript = mock(ExecutableScript.class);
        when(scriptService.executable(any(Script.class), eq(ScriptContext.Standard.SEARCH), eq(Collections.emptyMap())))
                .thenReturn(executableScript);

        XContentBuilder builder = jsonBuilder();
        String query = new TermQueryBuilder("field", "{{_user.username}}").toXContent(builder, ToXContent.EMPTY_PARAMS).string();
        Script script = new Script(query, ScriptService.ScriptType.INLINE, null, Collections.singletonMap("custom", "value"));
        builder = jsonBuilder().startObject().field("template");
        script.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference querySource = builder.endObject().bytes();

        securityIndexSearcherWrapper.evaluateTemplate(querySource);
        ArgumentCaptor<Script> argument = ArgumentCaptor.forClass(Script.class);
        verify(scriptService).executable(argument.capture(), eq(ScriptContext.Standard.SEARCH), eq(Collections.emptyMap()));
        Script usedScript = argument.getValue();
        assertThat(usedScript.getScript(), equalTo(script.getScript()));
        assertThat(usedScript.getType(), equalTo(script.getType()));
        assertThat(usedScript.getLang(), equalTo("mustache"));
        assertThat(usedScript.getContentType(), equalTo(script.getContentType()));
        assertThat(usedScript.getParams().size(), equalTo(2));
        assertThat(usedScript.getParams().get("custom"), equalTo("value"));

        Map<String, Object> userModel = new HashMap<>();
        userModel.put("username", user.principal());
        userModel.put("full_name", user.fullName());
        userModel.put("email", user.email());
        userModel.put("roles", Arrays.asList(user.roles()));
        userModel.put("metadata", user.metadata());
        assertThat(usedScript.getParams().get("_user"), equalTo(userModel));

    }

    public void testSkipTemplating() throws Exception {
        securityIndexSearcherWrapper =
                new SecurityIndexSearcherWrapper(indexSettings, null, mapperService, null, threadContext, licenseState, scriptService);
        XContentBuilder builder = jsonBuilder();
        BytesReference querySource =  new TermQueryBuilder("field", "value").toXContent(builder, ToXContent.EMPTY_PARAMS).bytes();
        BytesReference result = securityIndexSearcherWrapper.evaluateTemplate(querySource);
        assertThat(result, sameInstance(querySource));
        verifyZeroInteractions(scriptService);
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
        public float getValueForNormalization() throws IOException {
            return weight.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float boost) {
            weight.normalize(norm, boost);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            assertTrue(seenLeaves.add(context.reader().getCoreCacheKey()));
            return weight.scorer(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context)
                throws IOException {
            assertTrue(seenLeaves.add(context.reader().getCoreCacheKey()));
            return weight.bulkScorer(context);
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
        public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
            return new CreateScorerOnceWeight(query.createWeight(searcher, needsScores));
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

        DirectoryReader reader = DirectoryReader.open(w);
        IndexSettings settings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
        BitsetFilterCache.Listener listener = new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {

            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        };
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

    public void testVerifyRoleQuery() throws Exception {
        QueryBuilder queryBuilder1 = new TermsQueryBuilder("field", "val1", "val2");
        SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder1);

        QueryBuilder queryBuilder2 = new TermsQueryBuilder("field", new TermsLookup("_index", "_type", "_id", "_path"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder2));
        assertThat(e.getMessage(), equalTo("terms query with terms lookup isn't supported as part of a role query"));

        QueryBuilder queryBuilder3 = new GeoShapeQueryBuilder("field", "_id", "_type");
        e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder3));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder4 = new HasChildQueryBuilder("_type", new MatchAllQueryBuilder(), ScoreMode.None);
        e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder4));
        assertThat(e.getMessage(), equalTo("has_child query isn't support as part of a role query"));

        QueryBuilder queryBuilder5 = new HasParentQueryBuilder("_type", new MatchAllQueryBuilder(), false);
        e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder5));
        assertThat(e.getMessage(), equalTo("has_parent query isn't support as part of a role query"));

        QueryBuilder queryBuilder6 = new BoolQueryBuilder().must(new GeoShapeQueryBuilder("field", "_id", "_type"));
        e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder6));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder7 = new ConstantScoreQueryBuilder(new GeoShapeQueryBuilder("field", "_id", "_type"));
        e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder7));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder8 = new FunctionScoreQueryBuilder(new GeoShapeQueryBuilder("field", "_id", "_type"));
        e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder8));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));

        QueryBuilder queryBuilder9 = new BoostingQueryBuilder(new GeoShapeQueryBuilder("field", "_id", "_type"),
                new MatchAllQueryBuilder());
        e = expectThrows(IllegalArgumentException.class, () -> SecurityIndexSearcherWrapper.verifyRoleQuery(queryBuilder9));
        assertThat(e.getMessage(), equalTo("geoshape query referring to indexed shapes isn't support as part of a role query"));
    }

    public void testFailIfQueryUsesClient() throws Exception {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        QueryRewriteContext context = new QueryRewriteContext(null, mapperService, scriptService, null, client, null, null);
        QueryBuilder queryBuilder1 = new TermsQueryBuilder("field", "val1", "val2");
        SecurityIndexSearcherWrapper.failIfQueryUsesClient(queryBuilder1, context);

        QueryBuilder queryBuilder2 = new TermsQueryBuilder("field", new TermsLookup("_index", "_type", "_id", "_path"));
        Exception e = expectThrows(IllegalStateException.class,
                () -> SecurityIndexSearcherWrapper.failIfQueryUsesClient(queryBuilder2, context));
        assertThat(e.getMessage(), equalTo("role queries are not allowed to execute additional requests"));
    }
}
