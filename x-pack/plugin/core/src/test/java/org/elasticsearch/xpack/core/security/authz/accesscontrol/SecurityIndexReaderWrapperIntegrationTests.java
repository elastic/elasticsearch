/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SecurityIndexReaderWrapperIntegrationTests extends AbstractBuilderTestCase {

    public void testDLS() throws Exception {
        ShardId shardId = new ShardId("_index", "_na_", 0);
        MappingLookup mappingLookup = createMappingLookup(List.of(new KeywordFieldType("field")));
        ScriptService scriptService = mock(ScriptService.class);

        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);

        final Authentication authentication = AuthenticationTestHelper.builder().build();
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final long nowInMillis = randomNonNegativeLong();
        SearchExecutionContext realSearchExecutionContext = new SearchExecutionContext(
            shardId.id(),
            0,
            indexSettings,
            null,
            null,
            null,
            mappingLookup,
            null,
            null,
            parserConfig(),
            writableRegistry(),
            client,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );
        SearchExecutionContext searchExecutionContext = spy(realSearchExecutionContext);
        DocumentSubsetBitsetCache bitsetCache = new DocumentSubsetBitsetCache(Settings.EMPTY, Executors.newSingleThreadExecutor());
        final MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);

        Directory directory = newDirectory();
        IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE));

        int numValues = scaledRandomIntBetween(2, 16);
        String[] values = new String[numValues];
        for (int i = 0; i < numValues; i++) {
            values[i] = "value" + i;
        }
        int[] valuesHitCount = new int[numValues];

        int numDocs = scaledRandomIntBetween(32, 128);
        int commitAfter = scaledRandomIntBetween(1, numDocs);
        logger.info(
            "Going to index [{}] documents with [{}] unique values and commit after [{}] documents have been indexed",
            numDocs,
            numValues,
            commitAfter
        );

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
            String termQuery = "{\"term\": {\"field\": \"" + values[i] + "\"} }";
            IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(
                FieldPermissions.DEFAULT,
                DocumentPermissions.filteredBy(singleton(new BytesArray(termQuery)))
            );
            SecurityIndexReaderWrapper wrapper = new SecurityIndexReaderWrapper(
                s -> searchExecutionContext,
                bitsetCache,
                securityContext,
                licenseState,
                scriptService
            ) {

                @Override
                protected IndicesAccessControl getIndicesAccessControl() {
                    return new IndicesAccessControl(true, singletonMap("_index", indexAccessControl));
                }
            };

            ParsedQuery parsedQuery = new ParsedQuery(new TermQuery(new Term("field", values[i])));
            when(searchExecutionContext.toQuery(new TermsQueryBuilder("field", values[i]))).thenReturn(parsedQuery);

            DirectoryReader wrappedDirectoryReader = wrapper.apply(directoryReader);
            IndexSearcher indexSearcher = new ContextIndexSearcher(
                wrappedDirectoryReader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                true
            );

            int expectedHitCount = valuesHitCount[i];
            logger.info("Going to verify hit count with query [{}] with expected total hits [{}]", parsedQuery.query(), expectedHitCount);

            TotalHitCountCollector countCollector = new TotalHitCountCollector();
            indexSearcher.search(new MatchAllDocsQuery(), countCollector);

            assertThat(countCollector.getTotalHits(), equalTo(expectedHitCount));
            assertThat(wrappedDirectoryReader.numDocs(), equalTo(expectedHitCount));
        }

        bitsetCache.close();
        directoryReader.close();
        directory.close();
    }

    public void testDLSWithLimitedPermissions() throws Exception {
        ShardId shardId = new ShardId("_index", "_na_", 0);
        MappingLookup mappingLookup = createMappingLookup(
            List.of(new KeywordFieldType("field"), new KeywordFieldType("f1"), new KeywordFieldType("f2"))
        );
        ScriptService scriptService = mock(ScriptService.class);

        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        final boolean noFilteredIndexPermissions = randomBoolean();
        boolean restrictiveLimitedIndexPermissions = false;
        if (noFilteredIndexPermissions == false) {
            restrictiveLimitedIndexPermissions = randomBoolean();
        }
        Set<BytesReference> queries = new HashSet<>();
        queries.add(new BytesArray("{\"terms\" : { \"f2\" : [\"fv22\"] } }"));
        queries.add(new BytesArray("{\"terms\" : { \"f2\" : [\"fv32\"] } }"));
        IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(
            FieldPermissions.DEFAULT,
            DocumentPermissions.filteredBy(queries)
        );
        queries = singleton(new BytesArray("{\"terms\" : { \"f1\" : [\"fv11\", \"fv21\", \"fv31\"] } }"));
        if (restrictiveLimitedIndexPermissions) {
            queries = singleton(new BytesArray("{\"terms\" : { \"f1\" : [\"fv11\", \"fv31\"] } }"));
        }
        IndicesAccessControl.IndexAccessControl limitedIndexAccessControl = new IndicesAccessControl.IndexAccessControl(
            FieldPermissions.DEFAULT,
            DocumentPermissions.filteredBy(queries)
        );
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            shardId.getIndex(),
            Settings.builder().put(IndexSettings.ALLOW_UNMAPPED.getKey(), false).build()
        );
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final long nowInMillis = randomNonNegativeLong();
        SearchExecutionContext realSearchExecutionContext = new SearchExecutionContext(
            shardId.id(),
            0,
            indexSettings,
            null,
            null,
            null,
            mappingLookup,
            null,
            null,
            parserConfig(),
            writableRegistry(),
            client,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );
        SearchExecutionContext searchExecutionContext = spy(realSearchExecutionContext);
        DocumentSubsetBitsetCache bitsetCache = new DocumentSubsetBitsetCache(Settings.EMPTY, Executors.newSingleThreadExecutor());

        final MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        SecurityIndexReaderWrapper wrapper = new SecurityIndexReaderWrapper(
            s -> searchExecutionContext,
            bitsetCache,
            securityContext,
            licenseState,
            scriptService
        ) {

            @Override
            protected IndicesAccessControl getIndicesAccessControl() {
                IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, singletonMap("_index", indexAccessControl));
                if (noFilteredIndexPermissions) {
                    return indicesAccessControl;
                }
                IndicesAccessControl limitedByIndicesAccessControl = new IndicesAccessControl(
                    true,
                    singletonMap("_index", limitedIndexAccessControl)
                );
                return indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
            }
        };

        Directory directory = newDirectory();
        IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE));

        Document doc1 = new Document();
        doc1.add(new StringField("f1", "fv11", Store.NO));
        doc1.add(new StringField("f2", "fv12", Store.NO));
        iw.addDocument(doc1);
        Document doc2 = new Document();
        doc2.add(new StringField("f1", "fv21", Store.NO));
        doc2.add(new StringField("f2", "fv22", Store.NO));
        iw.addDocument(doc2);
        Document doc3 = new Document();
        doc3.add(new StringField("f1", "fv31", Store.NO));
        doc3.add(new StringField("f2", "fv32", Store.NO));
        iw.addDocument(doc3);
        iw.commit();
        iw.close();

        DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shardId);
        DirectoryReader wrappedDirectoryReader = wrapper.apply(directoryReader);
        IndexSearcher indexSearcher = new ContextIndexSearcher(
            wrappedDirectoryReader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true
        );

        ScoreDoc[] hits = indexSearcher.search(new MatchAllDocsQuery(), 1000).scoreDocs;
        Set<Integer> actualDocIds = new HashSet<>();
        for (ScoreDoc doc : hits) {
            actualDocIds.add(doc.doc);
        }

        if (noFilteredIndexPermissions) {
            assertThat(actualDocIds, containsInAnyOrder(1, 2));
        } else {
            if (restrictiveLimitedIndexPermissions) {
                assertThat(actualDocIds, containsInAnyOrder(2));
            } else {
                assertThat(actualDocIds, containsInAnyOrder(1, 2));
            }
        }

        bitsetCache.close();
        directoryReader.close();
        directory.close();
    }

    private static MappingLookup createMappingLookup(List<MappedFieldType> concreteFields) {
        List<FieldMapper> mappers = concreteFields.stream().map(MockFieldMapper::new).collect(Collectors.toList());
        return MappingLookup.fromMappers(Mapping.EMPTY, mappers, emptyList(), emptyList());
    }
}
