/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.percolator;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PercolateQueryBuilderTests extends AbstractQueryTestCase<PercolateQueryBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] {
        PercolateQueryBuilder.DOCUMENT_FIELD.getPreferredName(),
        PercolateQueryBuilder.DOCUMENTS_FIELD.getPreferredName() };

    protected static String queryField = "field";
    protected static String aliasField = "alias";
    private static String docType;

    private String indexedDocumentIndex;
    private String indexedDocumentId;
    private String indexedDocumentRouting;
    private String indexedDocumentPreference;
    private Long indexedDocumentVersion;
    private List<BytesReference> documentSource;

    private boolean indexedDocumentExists = true;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(PercolatorPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        queryField = randomAlphaOfLength(4);
        aliasField = randomAlphaOfLength(4);

        docType = "_doc";
        mapperService.merge(
            docType,
            new CompressedXContent(
                Strings.toString(
                    PutMappingRequest.simpleMapping(queryField, "type=percolator", aliasField, "type=alias,path=" + queryField)
                )
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        mapperService.merge(
            docType,
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(TEXT_FIELD_NAME, "type=text"))),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected PercolateQueryBuilder doCreateTestQueryBuilder() {
        return doCreateTestQueryBuilder(randomBoolean());
    }

    private PercolateQueryBuilder doCreateTestQueryBuilder(boolean indexedDocument) {
        if (indexedDocument) {
            documentSource = Collections.singletonList(randomSource(new HashSet<>()));
        } else {
            int numDocs = randomIntBetween(1, 8);
            documentSource = new ArrayList<>(numDocs);
            Set<String> usedFields = new HashSet<>();
            for (int i = 0; i < numDocs; i++) {
                documentSource.add(randomSource(usedFields));
            }
        }

        PercolateQueryBuilder queryBuilder;
        if (indexedDocument) {
            indexedDocumentIndex = randomAlphaOfLength(4);
            indexedDocumentId = randomAlphaOfLength(4);
            indexedDocumentRouting = randomAlphaOfLength(4);
            indexedDocumentPreference = randomAlphaOfLength(4);
            indexedDocumentVersion = (long) randomIntBetween(0, Integer.MAX_VALUE);
            queryBuilder = new PercolateQueryBuilder(
                queryField,
                indexedDocumentIndex,
                indexedDocumentId,
                indexedDocumentRouting,
                indexedDocumentPreference,
                indexedDocumentVersion
            );
        } else {
            queryBuilder = new PercolateQueryBuilder(queryField, documentSource, XContentType.JSON);
        }
        if (randomBoolean()) {
            queryBuilder.setName(randomAlphaOfLength(4));
        }
        return queryBuilder;
    }

    /**
     * we don't want to shuffle the "document" field internally in {@link #testFromXContent()} because even though the
     * documents would be functionally the same, their {@link BytesReference} representation isn't and thats what we
     * compare when check for equality of the original and the shuffled builder
     */
    @Override
    protected String[] shuffleProtectedFields() {
        return SHUFFLE_PROTECTED_FIELDS;
    }

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {
        assertThat(getRequest.index(), Matchers.equalTo(indexedDocumentIndex));
        assertThat(getRequest.id(), Matchers.equalTo(indexedDocumentId));
        assertThat(getRequest.routing(), Matchers.equalTo(indexedDocumentRouting));
        assertThat(getRequest.preference(), Matchers.equalTo(indexedDocumentPreference));
        assertThat(getRequest.version(), Matchers.equalTo(indexedDocumentVersion));
        if (indexedDocumentExists) {
            return new GetResponse(
                new GetResult(
                    indexedDocumentIndex,
                    indexedDocumentId,
                    0,
                    1,
                    0L,
                    true,
                    documentSource.iterator().next(),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
        } else {
            return new GetResponse(
                new GetResult(
                    indexedDocumentIndex,
                    indexedDocumentId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    false,
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
        }
    }

    @Override
    protected void doAssertLuceneQuery(PercolateQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, Matchers.instanceOf(PercolateQuery.class));
        PercolateQuery percolateQuery = (PercolateQuery) query;
        assertThat(percolateQuery.getDocuments(), Matchers.equalTo(documentSource));
    }

    @Override
    public void testMustRewrite() throws IOException {
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> pqb.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), equalTo("query builder must be rewritten first"));
        QueryBuilder rewrite = rewriteAndFetch(pqb, createSearchExecutionContext());
        PercolateQueryBuilder geoShapeQueryBuilder = new PercolateQueryBuilder(pqb.getField(), documentSource, XContentType.JSON);
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testIndexedDocumentDoesNotExist() throws IOException {
        indexedDocumentExists = false;
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> rewriteAndFetch(pqb, createSearchExecutionContext())
        );
        String expectedString = "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentId + "] couldn't be found";
        assertThat(e.getMessage(), equalTo(expectedString));
    }

    @Override
    protected Map<String, String> getObjectsHoldingArbitraryContent() {
        // document contains arbitrary content, no error expected when an object is added to it
        final Map<String, String> objects = new HashMap<>();
        objects.put(PercolateQueryBuilder.DOCUMENT_FIELD.getPreferredName(), null);
        objects.put(PercolateQueryBuilder.DOCUMENTS_FIELD.getPreferredName(), null);
        return objects;
    }

    public void testRequiredParameters() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder(null, new BytesArray("{}"), XContentType.JSON);
        });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new PercolateQueryBuilder("_field", (List<BytesReference>) null, XContentType.JSON)
        );
        assertThat(e.getMessage(), equalTo("[document] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> { new PercolateQueryBuilder(null, "_index", "_id", null, null, null); });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> { new PercolateQueryBuilder("_field", null, "_id", null, null, null); });
        assertThat(e.getMessage(), equalTo("[index] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> { new PercolateQueryBuilder("_field", "_index", null, null, null, null); });
        assertThat(e.getMessage(), equalTo("[id] is a required argument"));
    }

    public void testFromJsonNoDocumentType() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder queryBuilder = parseQuery(Strings.format("""
            {"percolate" : { "document": {}, "field":"%s"}}
            """, queryField));
        queryBuilder.toQuery(searchExecutionContext);
    }

    public void testFromJsonNoType() throws IOException {
        indexedDocumentIndex = randomAlphaOfLength(4);
        indexedDocumentId = randomAlphaOfLength(4);
        indexedDocumentVersion = Versions.MATCH_ANY;
        documentSource = Collections.singletonList(randomSource(new HashSet<>()));

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder queryBuilder = parseQuery(Strings.format("""
            {"percolate" : { "index": "%s", "id": "%s", "field":"%s"}}
            """, indexedDocumentIndex, indexedDocumentId, queryField));
        rewriteAndFetch(queryBuilder, searchExecutionContext).toQuery(searchExecutionContext);
    }

    public void testBothDocumentAndDocumentsSpecified() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(Strings.format("""
            {"percolate" : { "document": {}, "documents": [{}, {}], "field":"%s"}}
            """, queryField)));
        assertThat(e.getMessage(), containsString("The following fields are not allowed together: [document, documents]"));
    }

    private static BytesReference randomSource(Set<String> usedFields) {
        try {
            // If we create two source that have the same field, but these fields have different kind of values (str vs. lng) then
            // when these source get indexed, indexing can fail. To solve this test issue, we should generate source that
            // always have unique fields:
            Map<String, ?> source;
            boolean duplicateField;
            do {
                duplicateField = false;
                source = RandomDocumentPicks.randomSource(random());
                for (String field : source.keySet()) {
                    if (usedFields.add(field) == false) {
                        duplicateField = true;
                        break;
                    }
                }
            } while (duplicateField);

            XContentBuilder xContent = XContentFactory.jsonBuilder();
            xContent.map(source);
            return BytesReference.bytes(xContent);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Test that this query is never cacheable
     */
    @Override
    public void testCacheability() throws IOException {
        PercolateQueryBuilder queryBuilder = createTestQueryBuilder();
        SearchExecutionContext context = createSearchExecutionContext();
        assert context.isCacheable();
        QueryBuilder rewritten = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = doCreateTestQueryBuilder(true);
        QueryBuilder queryBuilder = Rewriteable.rewrite(builder, createSearchExecutionContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> queryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createSearchExecutionContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    public void testFieldAlias() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        PercolateQueryBuilder builder = doCreateTestQueryBuilder(false);
        QueryBuilder rewrittenBuilder = rewriteAndFetch(builder, searchExecutionContext);
        PercolateQuery query = (PercolateQuery) rewrittenBuilder.toQuery(searchExecutionContext);

        PercolateQueryBuilder aliasBuilder = new PercolateQueryBuilder(aliasField, builder.getDocuments(), builder.getXContentType());
        QueryBuilder rewrittenAliasBuilder = rewriteAndFetch(aliasBuilder, searchExecutionContext);
        PercolateQuery aliasQuery = (PercolateQuery) rewrittenAliasBuilder.toQuery(searchExecutionContext);

        assertEquals(query.getCandidateMatchesQuery(), aliasQuery.getCandidateMatchesQuery());
        assertEquals(query.getVerifiedMatchesQuery(), aliasQuery.getVerifiedMatchesQuery());
    }

    public void testSettingNameWhileRewriting() {
        String testName = "name1";
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        PercolateQueryBuilder percolateQueryBuilder = doCreateTestQueryBuilder(true);
        percolateQueryBuilder.setName(testName);

        QueryBuilder rewrittenQueryBuilder = percolateQueryBuilder.doRewrite(searchExecutionContext);

        assertEquals(testName, ((PercolateQueryBuilder) rewrittenQueryBuilder).getQueryName());
        assertNotEquals(rewrittenQueryBuilder, percolateQueryBuilder);
    }

    public void testSettingNameWhileRewritingWhenDocumentSupplierAndSourceNotNull() {
        Supplier<BytesReference> supplier = () -> new BytesArray("{\"test\": \"test\"}");
        String testName = "name1";
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        PercolateQueryBuilder percolateQueryBuilder = new PercolateQueryBuilder(queryField, supplier);
        percolateQueryBuilder.setName(testName);

        QueryBuilder rewrittenQueryBuilder = percolateQueryBuilder.doRewrite(searchExecutionContext);

        assertEquals(testName, ((PercolateQueryBuilder) rewrittenQueryBuilder).getQueryName());
        assertNotEquals(rewrittenQueryBuilder, percolateQueryBuilder);
    }

    public void testDisallowExpensiveQueries() {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(false);

        PercolateQueryBuilder queryBuilder = doCreateTestQueryBuilder(true);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> queryBuilder.toQuery(searchExecutionContext));
        assertEquals("[percolate] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }
}
