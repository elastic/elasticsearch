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

package org.elasticsearch.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PercolateQueryBuilderTests extends AbstractQueryTestCase<PercolateQueryBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] {
        PercolateQueryBuilder.DOCUMENT_FIELD.getPreferredName(),
        PercolateQueryBuilder.DOCUMENTS_FIELD.getPreferredName()
    };

    private static String queryField;
    private static String docType;

    private String indexedDocumentIndex;
    private String indexedDocumentType;
    private String indexedDocumentId;
    private String indexedDocumentRouting;
    private String indexedDocumentPreference;
    private Long indexedDocumentVersion;
    private List<BytesReference> documentSource;

    private boolean indexedDocumentExists = true;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        queryField = randomAlphaOfLength(4);
        String docType = "doc";
        mapperService.merge(docType, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(docType,
                queryField, "type=percolator"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapperService.merge(docType, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(docType,
                STRING_FIELD_NAME, "type=text"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        if (mapperService.getIndexSettings().isSingleType() == false) {
            PercolateQueryBuilderTests.docType = docType;
        }
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
            indexedDocumentType = "doc";
            indexedDocumentId = randomAlphaOfLength(4);
            indexedDocumentRouting = randomAlphaOfLength(4);
            indexedDocumentPreference = randomAlphaOfLength(4);
            indexedDocumentVersion = (long) randomIntBetween(0, Integer.MAX_VALUE);
            queryBuilder = new PercolateQueryBuilder(queryField, docType, indexedDocumentIndex, indexedDocumentType, indexedDocumentId,
                    indexedDocumentRouting, indexedDocumentPreference, indexedDocumentVersion);
        } else {
            queryBuilder = new PercolateQueryBuilder(queryField, docType, documentSource, XContentType.JSON);
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
        assertThat(getRequest.type(), Matchers.equalTo(indexedDocumentType));
        assertThat(getRequest.id(), Matchers.equalTo(indexedDocumentId));
        assertThat(getRequest.routing(), Matchers.equalTo(indexedDocumentRouting));
        assertThat(getRequest.preference(), Matchers.equalTo(indexedDocumentPreference));
        assertThat(getRequest.version(), Matchers.equalTo(indexedDocumentVersion));
        if (indexedDocumentExists) {
            return new GetResponse(
                    new GetResult(indexedDocumentIndex, indexedDocumentType, indexedDocumentId, 0L, true,
                            documentSource.iterator().next(), Collections.emptyMap())
            );
        } else {
            return new GetResponse(
                    new GetResult(indexedDocumentIndex, indexedDocumentType, indexedDocumentId, -1, false, null, Collections.emptyMap())
            );
        }
    }

    @Override
    protected void doAssertLuceneQuery(PercolateQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, Matchers.instanceOf(PercolateQuery.class));
        PercolateQuery percolateQuery = (PercolateQuery) query;
        assertThat(docType, Matchers.equalTo(queryBuilder.getDocumentType()));
        assertThat(percolateQuery.getDocuments(), Matchers.equalTo(documentSource));
    }

    @Override
    public void testMustRewrite() throws IOException {
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> pqb.toQuery(createShardContext()));
        assertThat(e.getMessage(), equalTo("query builder must be rewritten first"));
        QueryBuilder rewrite = rewriteAndFetch(pqb, createShardContext());
        PercolateQueryBuilder geoShapeQueryBuilder =
            new PercolateQueryBuilder(pqb.getField(), pqb.getDocumentType(), documentSource, XContentType.JSON);
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testIndexedDocumentDoesNotExist() throws IOException {
        indexedDocumentExists = false;
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> rewriteAndFetch(pqb,
            createShardContext()));
        String expectedString = "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentType + "/" +
                indexedDocumentId +  "] couldn't be found";
        assertThat(e.getMessage() , equalTo(expectedString));
    }

    @Override
    protected Set<String> getObjectsHoldingArbitraryContent() {
        //document contains arbitrary content, no error expected when an object is added to it
        return new HashSet<>(Arrays.asList(PercolateQueryBuilder.DOCUMENT_FIELD.getPreferredName(),
                PercolateQueryBuilder.DOCUMENTS_FIELD.getPreferredName()));
    }

    public void testRequiredParameters() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder(null, new BytesArray("{}"), XContentType.JSON);
        });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(IllegalArgumentException.class,
            () -> new PercolateQueryBuilder("_field", "_document_type", null, null));
        assertThat(e.getMessage(), equalTo("[document] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder(null, null, "_index", "_type", "_id", null, null, null);
        });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder("_field", "_document_type", null, "_type", "_id", null, null, null);
        });
        assertThat(e.getMessage(), equalTo("[index] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder("_field", "_document_type", "_index", null, "_id", null, null, null);
        });
        assertThat(e.getMessage(), equalTo("[type] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder("_field", "_document_type", "_index", "_type", null, null, null, null);
        });
        assertThat(e.getMessage(), equalTo("[id] is a required argument"));
    }

    public void testFromJsonNoDocumentType() throws IOException {
        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder queryBuilder = parseQuery("{\"percolate\" : { \"document\": {}, \"field\":\"" + queryField + "\"}}");
        if (indexVersionCreated.before(Version.V_6_0_0_alpha1)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> queryBuilder.toQuery(queryShardContext));
            assertThat(e.getMessage(), equalTo("[percolate] query is missing required [document_type] parameter"));
        } else {
            queryBuilder.toQuery(queryShardContext);
        }
    }

    public void testBothDocumentAndDocumentsSpecified() throws IOException {
        expectThrows(IllegalArgumentException.class,
            () -> parseQuery("{\"percolate\" : { \"document\": {}, \"documents\": [{}, {}], \"field\":\"" + queryField + "\"}}"));
    }

    public void testCreateNestedDocumentSearcher() throws Exception {
        int numNestedDocs = randomIntBetween(2, 8);
        List<ParseContext.Document> docs = new ArrayList<>(numNestedDocs);
        for (int i = 0; i < numNestedDocs; i++) {
            docs.add(new ParseContext.Document());
        }

        Collection<ParsedDocument> parsedDocument = Collections.singleton(
            new ParsedDocument(null, null, "_id", "_type", null, docs, null, null, null));
        Analyzer analyzer = new WhitespaceAnalyzer();
        IndexSearcher indexSearcher = PercolateQueryBuilder.createMultiDocumentSearcher(analyzer, parsedDocument);
        assertThat(indexSearcher.getIndexReader().numDocs(), equalTo(numNestedDocs));

        // ensure that any query get modified so that the nested docs are never included as hits:
        Query query = new MatchAllDocsQuery();
        BooleanQuery result = (BooleanQuery) indexSearcher.createNormalizedWeight(query, true).getQuery();
        assertThat(result.clauses().size(), equalTo(2));
        assertThat(result.clauses().get(0).getQuery(), sameInstance(query));
        assertThat(result.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(result.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));
    }

    public void testCreateMultiDocumentSearcher() throws Exception {
        int numDocs = randomIntBetween(2, 8);
        List<ParsedDocument> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(new ParsedDocument(null, null, "_id", "_type", null,
                Collections.singletonList(new ParseContext.Document()), null, null, null));
        }
        Analyzer analyzer = new WhitespaceAnalyzer();
        IndexSearcher indexSearcher = PercolateQueryBuilder.createMultiDocumentSearcher(analyzer, docs);
        assertThat(indexSearcher.getIndexReader().numDocs(), equalTo(numDocs));

        // ensure that any query get modified so that the nested docs are never included as hits:
        Query query = new MatchAllDocsQuery();
        BooleanQuery result = (BooleanQuery) indexSearcher.createNormalizedWeight(query, true).getQuery();
        assertThat(result.clauses().size(), equalTo(2));
        assertThat(result.clauses().get(0).getQuery(), sameInstance(query));
        assertThat(result.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(result.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));
    }

    public void testSerializationBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("P4AAAAAFZmllbGQEdHlwZQAAAAAAAA57ImZvbyI6ImJhciJ9AAAAAA==");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
            Version.V_5_1_1, Version.V_5_1_2, Version.V_5_2_0);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            PercolateQueryBuilder queryBuilder = new PercolateQueryBuilder(in);
            assertEquals("type", queryBuilder.getDocumentType());
            assertEquals("field", queryBuilder.getField());
            assertEquals("{\"foo\":\"bar\"}", queryBuilder.getDocuments().iterator().next().utf8ToString());
            assertEquals(XContentType.JSON, queryBuilder.getXContentType());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                queryBuilder.writeTo(out);
                assertArrayEquals(data, out.bytes().toBytesRef().bytes);
            }
        }
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
            return xContent.bytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isCachable(PercolateQueryBuilder queryBuilder) {
        return false;
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = doCreateTestQueryBuilder(true);
        QueryBuilder queryBuilder = Rewriteable.rewrite(builder, createShardContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> queryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createShardContext());
        builder.writeTo(new BytesStreamOutput(10));
    }
}
