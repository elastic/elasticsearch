/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Collections;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShieldIndexSearcherWrapperIntegrationTests extends ESTestCase {

    public void testDLS() throws Exception {
        ShardId shardId = new ShardId("_index", "_na_", 0);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.docMappers(anyBoolean())).thenReturn(Collections.emptyList());
        when(mapperService.simpleMatchToIndexNames(anyString()))
                .then(invocationOnMock -> Collections.singletonList((String) invocationOnMock.getArguments()[0]));

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(true, null,
                singleton(new BytesArray("{}")));
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        IndexSettings settings = IndexSettingsModule.newIndexSettings("_index", Settings.EMPTY);
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(settings, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {
            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        });
        SecurityLicenseState licenseState = mock(SecurityLicenseState.class);
        when(licenseState.documentAndFieldLevelSecurityEnabled()).thenReturn(true);
        ShieldIndexSearcherWrapper wrapper = new ShieldIndexSearcherWrapper(indexSettings, queryShardContext, mapperService,
                bitsetFilterCache, threadContext, licenseState) {

            @Override
            protected QueryShardContext copyQueryShardContext(QueryShardContext context) {
                return queryShardContext;
            }

            @Override
            protected IndicesAccessControl getIndicesAccessControl() {
                return new IndicesAccessControl(true, singletonMap("_index", indexAccessControl));
            }
        };

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
        logger.info("Going to index [{}] documents with [{}] unique values and commit after [{}] documents have been indexed",
                numDocs, numValues, commitAfter);

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
            when(queryShardContext.parse(any(BytesReference.class))).thenReturn(parsedQuery);
            DirectoryReader wrappedDirectoryReader = wrapper.wrap(directoryReader);
            IndexSearcher indexSearcher = wrapper.wrap(new IndexSearcher(wrappedDirectoryReader));

            int expectedHitCount = valuesHitCount[i];
            logger.info("Going to verify hit count with query [{}] with expected total hits [{}]", parsedQuery.query(), expectedHitCount);
            TotalHitCountCollector countCollector = new TotalHitCountCollector();
            indexSearcher.search(new MatchAllDocsQuery(), countCollector);
            assertThat(countCollector.getTotalHits(), equalTo(expectedHitCount));
            assertThat(wrappedDirectoryReader.numDocs(), equalTo(expectedHitCount));
        }

        bitsetFilterCache.close();
        directoryReader.close();
        directory.close();
    }

}
