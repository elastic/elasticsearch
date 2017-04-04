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
package org.elasticsearch.search.collapse;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitBuilderTests;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollapseBuilderTests extends AbstractSearchTestCase {
    private static NamedWriteableRegistry namedWriteableRegistry;

    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    public static CollapseBuilder randomCollapseBuilder() {
        CollapseBuilder builder = new CollapseBuilder(randomAlphaOfLength(10));
        builder.setMaxConcurrentGroupRequests(randomIntBetween(1, 48));
        if (randomBoolean()) {
            InnerHitBuilder innerHit = InnerHitBuilderTests.randomInnerHits(false, false);
            builder.setInnerHits(innerHit);
        }
        return builder;
    }

    public void testSerialization() throws IOException {
        CollapseBuilder testBuilder = randomCollapseBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testBuilder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                CollapseBuilder deserializedBuilder = new CollapseBuilder(in);
                assertEquals(deserializedBuilder, testBuilder);
                assertEquals(deserializedBuilder.hashCode(), testBuilder.hashCode());
                assertNotSame(deserializedBuilder, testBuilder);
            }
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        CollapseBuilder instance1 = randomCollapseBuilder();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(instance1, this::copyBuilder);
    }

    //we use the streaming infra to create a copy of the builder provided as argument
    private CollapseBuilder copyBuilder(CollapseBuilder original) throws IOException {
        return ESTestCase.copyWriteable(original, namedWriteableRegistry, CollapseBuilder::new);
    }

    private SearchContext mockSearchContext() {
        SearchContext context = mock(SearchContext.class);
        QueryShardContext shardContext = mock(QueryShardContext.class);
        when(context.getQueryShardContext()).thenReturn(shardContext);
        when(context.scrollContext()).thenReturn(null);
        when(context.rescore()).thenReturn(null);
        when(context.searchAfter()).thenReturn(null);
        return context;
    }

    public void testBuild() throws IOException {
        Directory dir = new RAMDirectory();
        try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())))) {
            writer.commit();
        }
        SearchContext searchContext = mockSearchContext();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            when(searchContext.getQueryShardContext().getIndexReader()).thenReturn(reader);
            MappedFieldType numberFieldType =
                new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            MappedFieldType keywordFieldType =
                new KeywordFieldMapper.KeywordFieldType();
            for (MappedFieldType fieldType : new MappedFieldType[] {numberFieldType, keywordFieldType}) {
                fieldType.setName("field");
                fieldType.setHasDocValues(true);
                when(searchContext.getQueryShardContext().fieldMapper("field")).thenReturn(fieldType);
                CollapseBuilder builder = new CollapseBuilder("field");
                CollapseContext collapseContext = builder.build(searchContext);
                assertEquals(collapseContext.getFieldType(), fieldType);

                fieldType.setIndexOptions(IndexOptions.NONE);
                collapseContext = builder.build(searchContext);
                assertEquals(collapseContext.getFieldType(), fieldType);

                fieldType.setHasDocValues(false);
                SearchContextException exc = expectThrows(SearchContextException.class, () -> builder.build(searchContext));
                assertEquals(exc.getMessage(), "cannot collapse on field `field` without `doc_values`");

                fieldType.setHasDocValues(true);
                builder.setInnerHits(new InnerHitBuilder());
                exc = expectThrows(SearchContextException.class, () -> builder.build(searchContext));
                assertEquals(exc.getMessage(),
                    "cannot expand `inner_hits` for collapse field `field`, " +
                        "only indexed field can retrieve `inner_hits`");
            }
        }
    }

    public void testBuildWithSearchContextExceptions() throws IOException {
        SearchContext context = mockSearchContext();
        {
            CollapseBuilder builder = new CollapseBuilder("unknown_field");
            SearchContextException exc = expectThrows(SearchContextException.class, () -> builder.build(context));
            assertEquals(exc.getMessage(), "no mapping found for `unknown_field` in order to collapse on");
        }

        {
            MappedFieldType fieldType = new MappedFieldType() {
                @Override
                public MappedFieldType clone() {
                    return null;
                }

                @Override
                public String typeName() {
                    return null;
                }

                @Override
                public Query termQuery(Object value, QueryShardContext context) {
                    return null;
                }
            };
            fieldType.setName("field");
            fieldType.setHasDocValues(true);
            when(context.getQueryShardContext().fieldMapper("field")).thenReturn(fieldType);
            CollapseBuilder builder = new CollapseBuilder("field");
            SearchContextException exc = expectThrows(SearchContextException.class, () -> builder.build(context));
            assertEquals(exc.getMessage(), "unknown type for collapse field `field`, only keywords and numbers are accepted");
        }
    }
}
