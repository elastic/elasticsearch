/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.indices.InternalIndicesLifecycle;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShieldIndexSearcherWrapperTests extends ESTestCase {

    private ShardId shardId;
    private TransportRequest request;
    private MapperService mapperService;
    private ShieldIndexSearcherWrapper shieldIndexSearcherWrapper;
    private ElasticsearchDirectoryReader esIn;

    @Before
    public void before() throws Exception {
        Index index = new Index("_index");
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
            AnalysisService analysisService = new AnalysisService(index, settings);
        SimilarityLookupService similarityLookupService = new SimilarityLookupService(index, settings);
        ScriptService scriptService = mock(ScriptService.class);
        mapperService = new MapperService(index, settings, analysisService, similarityLookupService, scriptService);

        shardId = new ShardId(index, 0);
        InternalIndicesLifecycle indicesLifecycle = new InternalIndicesLifecycle(settings);
        shieldIndexSearcherWrapper = new ShieldIndexSearcherWrapper(shardId, settings, null, indicesLifecycle, mapperService);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        indicesLifecycle.afterIndexShardPostRecovery(indexShard);

        request = new TransportRequest.Empty();
        RequestContext.setCurrent(new RequestContext(request));

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
        mapperService.merge("type", new CompressedXContent(mappingSource.string()), false, false);

        IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(true, ImmutableSet.<String>of(), null);
        request.putInContext(InternalAuthorizationService.INDICES_PERMISSIONS_KEY, new IndicesAccessControl(true, ImmutableMap.of("_index", indexAccessControl)));

        FieldSubsetReader.FieldSubsetDirectoryReader result = (FieldSubsetReader.FieldSubsetDirectoryReader) shieldIndexSearcherWrapper.wrap(esIn);
        assertThat(result.getFieldNames().size(), equalTo(11));
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
        assertThat(result.getFieldNames().contains("_all"), is(false)); // _all contains actual user data and therefor can't be included by default
    }

    public void testWildcards() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("field1_a").field("type", "string").endObject()
                    .startObject("field1_b").field("type", "string").endObject()
                    .startObject("field1_c").field("type", "string").endObject()
                    .startObject("field2_a").field("type", "string").endObject()
                    .startObject("field2_b").field("type", "string").endObject()
                    .startObject("field2_c").field("type", "string").endObject()
                .endObject().endObject().endObject();
        mapperService.merge("type", new CompressedXContent(mappingSource.string()), false, false);

        assertResolvedFields("field1*", "field1_a", "field1_b", "field1_c");
        assertResolvedFields("field2*", "field2_a", "field2_b", "field2_c");
    }

    public void testDotNotion() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("bar").field("type", "string").endObject()
                        .startObject("baz").field("type", "string").endObject()
                    .endObject()
                .endObject()
                .startObject("bar")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("foo").field("type", "string").endObject()
                        .startObject("baz").field("type", "string").endObject()
                    .endObject()
                .endObject()
                .startObject("baz")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("bar").field("type", "string").endObject()
                        .startObject("foo").field("type", "string").endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject();
        mapperService.merge("type", new CompressedXContent(mappingSource.string()), false, false);

        assertResolvedFields("foo.bar", "foo.bar");
        assertResolvedFields("bar.baz", "bar.baz");
        assertResolvedFields("foo.*", "foo.bar", "foo.baz");
        assertResolvedFields("baz.*", "baz.bar", "baz.foo");
    }

    public void testParentChild() throws Exception {
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("parent1")
                .startObject("properties")
                    .startObject("field").field("type", "string").endObject()
                .endObject()
                .endObject().endObject();
        mapperService.merge("parent1", new CompressedXContent(mappingSource.string()), false, false);
        mappingSource = jsonBuilder().startObject().startObject("child1")
                .startObject("properties")
                    .startObject("field").field("type", "string").endObject()
                .endObject()
                .startObject("_parent")
                    .field("type", "parent1")
                .endObject()
                .endObject().endObject();
        mapperService.merge("child1", new CompressedXContent(mappingSource.string()), false, false);
        mappingSource = jsonBuilder().startObject().startObject("child2")
                .startObject("properties")
                    .startObject("field").field("type", "string").endObject()
                .endObject()
                .startObject("_parent")
                    .field("type", "parent1")
                .endObject()
                .endObject().endObject();
        mapperService.merge("child2", new CompressedXContent(mappingSource.string()), false, false);
        mappingSource = jsonBuilder().startObject().startObject("parent2")
                .startObject("properties")
                .startObject("field").field("type", "string").endObject()
                .endObject()
                .endObject().endObject();
        mapperService.merge("parent2", new CompressedXContent(mappingSource.string()), false, false);
        mappingSource = jsonBuilder().startObject().startObject("child3")
                .startObject("properties")
                    .startObject("field").field("type", "string").endObject()
                .endObject()
                .startObject("_parent")
                    .field("type", "parent2")
                .endObject()
                .endObject().endObject();
        mapperService.merge("child3", new CompressedXContent(mappingSource.string()), false, false);

        assertResolvedFields("field1", "field1", ParentFieldMapper.joinField("parent1"), ParentFieldMapper.joinField("parent2"));
    }

    private void assertResolvedFields(String expression, String... expectedFields) {
        IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(true, ImmutableSet.of(expression), null);
        request.putInContext(InternalAuthorizationService.INDICES_PERMISSIONS_KEY, new IndicesAccessControl(true, ImmutableMap.of("_index", indexAccessControl)));
        FieldSubsetReader.FieldSubsetDirectoryReader result = (FieldSubsetReader.FieldSubsetDirectoryReader) shieldIndexSearcherWrapper.wrap(esIn);
        assertThat(result.getFieldNames().size() - shieldIndexSearcherWrapper.getAllowedMetaFields().size(), equalTo(expectedFields.length));
        for (String expectedField : expectedFields) {
            assertThat(result.getFieldNames().contains(expectedField), is(true));
        }
    }

}
