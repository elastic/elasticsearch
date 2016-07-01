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

package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class NestedAggregatorTests extends ESSingleNodeTestCase {
    public void testResetRootDocId() throws Exception {
        Directory directory = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, iwc);

        List<Document> documents = new ArrayList<>();

        // 1 segment with, 1 root document, with 3 nested sub docs
        Document document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        indexWriter.addDocuments(documents);
        indexWriter.commit();

        documents.clear();
        // 1 segment with:
        // 1 document, with 1 nested subdoc
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#2", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#2", UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        indexWriter.addDocuments(documents);
        documents.clear();
        // and 1 document, with 1 nested subdoc
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#3", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "type#3", UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
        documents.add(document);
        indexWriter.addDocuments(documents);

        indexWriter.commit();
        indexWriter.close();

        IndexService indexService = createIndex("test");
        DirectoryReader directoryReader = DirectoryReader.open(directory);
        directoryReader = ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(directoryReader);

        indexService.mapperService().merge("test", new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef("test", "nested_field", "type=nested").string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        SearchContext searchContext = createSearchContext(indexService);
        AggregationContext context = new AggregationContext(searchContext);

        AggregatorFactories.Builder builder = AggregatorFactories.builder();
        NestedAggregationBuilder factory = new NestedAggregationBuilder("test", "nested_field");
        builder.addAggregator(factory);
        AggregatorFactories factories = builder.build(context, null);
        searchContext.aggregations(new SearchContextAggregations(factories));
        Aggregator[] aggs = factories.createTopLevelAggregators();
        BucketCollector collector = BucketCollector.wrap(Arrays.asList(aggs));
        collector.preCollection();
        // A regular search always exclude nested docs, so we use NonNestedDocsFilter.INSTANCE here (otherwise MatchAllDocsQuery would be sufficient)
        // We exclude root doc with uid type#2, this will trigger the bug if we don't reset the root doc when we process a new segment, because
        // root doc type#3 and root doc type#1 have the same segment docid
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(Queries.newNonNestedFilter(), Occur.MUST);
        bq.add(new TermQuery(new Term(UidFieldMapper.NAME, "type#2")), Occur.MUST_NOT);
        searcher.search(new ConstantScoreQuery(bq.build()), collector);
        collector.postCollection();

        Nested nested = (Nested) aggs[0].buildAggregation(0);
        // The bug manifests if 6 docs are returned, because currentRootDoc isn't reset the previous child docs from the first segment are emitted as hits.
        assertThat(nested.getDocCount(), equalTo(4L));

        directoryReader.close();
        directory.close();
    }

}
