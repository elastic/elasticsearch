/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShieldIndexSearcherWrapperIntegrationTests extends ESTestCase {

    public void testDLS() throws Exception {
        ShardId shardId = new ShardId("_index", 0);
        EngineConfig engineConfig = new EngineConfig(shardId, null, null, Settings.EMPTY, null, null, null, null, null, null, null, null, null, null, null, QueryCachingPolicy.ALWAYS_CACHE, null, null); // can't mock...

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.docMappers(anyBoolean())).thenReturn(Collections.emptyList());
        when(mapperService.simpleMatchToIndexNames(anyString())).then(new Answer<Collection<String>>() {
            @Override
            public Collection<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return Collections.singletonList((String) invocationOnMock.getArguments()[0]);
            }
        });

        TransportRequest request = new TransportRequest.Empty();
        RequestContext.setCurrent(new RequestContext(request));
        IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(true, null, ImmutableSet.of(new BytesArray("{}")));
        request.putInContext(InternalAuthorizationService.INDICES_PERMISSIONS_KEY, new IndicesAccessControl(true, ImmutableMap.of("_index", indexAccessControl)));
        IndexQueryParserService parserService = mock(IndexQueryParserService.class);

        IndicesLifecycle indicesLifecycle = mock(IndicesLifecycle.class);
        BitsetFilterCache bitsetFilterCache = mock(BitsetFilterCache.class);
        when(bitsetFilterCache.getBitSetProducer(Matchers.any(Query.class))).then(new Answer<BitSetProducer>() {
            @Override
            public BitSetProducer answer(InvocationOnMock invocationOnMock) throws Throwable {
                final Query query = (Query) invocationOnMock.getArguments()[0];
                return context -> {
                    IndexSearcher searcher = new IndexSearcher(context);
                    searcher.setQueryCache(null);
                    Weight weight = searcher.createNormalizedWeight(query, false);
                    DocIdSetIterator it = weight.scorer(context);
                    if (it != null) {
                        int maxDoc = context.reader().maxDoc();
                        BitSet bitSet = randomBoolean() ? new SparseFixedBitSet(maxDoc) : new FixedBitSet(maxDoc);
                        bitSet.or(it);
                        return bitSet;
                    } else {
                        return null;
                    }
                };
            }
        });
        ShieldIndexSearcherWrapper wrapper = new ShieldIndexSearcherWrapper(
                shardId, Settings.EMPTY, parserService, indicesLifecycle, mapperService, bitsetFilterCache
        );

        Directory directory = newDirectory();
        IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
        );

        int numValues = scaledRandomIntBetween(2, 16);
        String[] values = new String[numValues];
        for (int i = 0; i < numValues; i++) {
            values[i] = "value" + i;
        }
        int[] valuesHitCount = new int[numValues];

        int numDocs = scaledRandomIntBetween(32, 128);
        int commitAfter = scaledRandomIntBetween(1, numDocs);
        logger.info("Going to index [{}] documents with [{}] unique values and commit after [{}] documents have been indexed", numDocs, numValues, commitAfter);

        for (int doc = 1; doc <= numDocs; doc++) {
            int valueIndex = (numValues - 1) % doc;

            Document document = new Document();
            String id = String.valueOf(doc);
            document.add(new StringField("id", id, Field.Store.NO));
            String value = values[valueIndex];
            document.add(new StringField("field", value, Field.Store.NO));
            iw.addDocument(document);
            if (doc % 11 == 0) {
                iw.deleteDocuments(new Term("id", id));
            } else {
                if (commitAfter % commitAfter == 0) {
                    iw.commit();
                }
                valuesHitCount[valueIndex]++;
            }
        }
        iw.close();
        StringBuilder valueToHitCountOutput = new StringBuilder();
        for (int i = 0; i < numValues; i++) {
            valueToHitCountOutput.append(values[i]).append('\t').append(valuesHitCount[i]).append('\n');
        }
        logger.info("Value count matrix:\n{}", valueToHitCountOutput);

        DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shardId);
        for (int i = 0; i < numValues; i++) {
            ParsedQuery parsedQuery = new ParsedQuery(new TermQuery(new Term("field", values[i])));
            when(parserService.parse(any(BytesReference.class))).thenReturn(parsedQuery);
            DirectoryReader wrappedDirectoryReader = wrapper.wrap(directoryReader);
            IndexSearcher indexSearcher = wrapper.wrap(engineConfig, new IndexSearcher(wrappedDirectoryReader));

            int expectedHitCount = valuesHitCount[i];
            logger.info("Going to verify hit count with query [{}] with expected total hits [{}]", parsedQuery.query(), expectedHitCount);
            TotalHitCountCollector countCollector = new TotalHitCountCollector();
            indexSearcher.search(new MatchAllDocsQuery(), countCollector);
            assertThat(countCollector.getTotalHits(), equalTo(expectedHitCount));
            assertThat(wrappedDirectoryReader.numDocs(), equalTo(expectedHitCount));
        }

        directoryReader.close();
        directory.close();
    }

}
