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

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.script.Script;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PercolateQueryBuilderTests extends AbstractQueryTestCase<PercolateQueryBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] { PercolateQueryBuilder.DOCUMENT_FIELD.getPreferredName()};

    private static String queryField;
    private static String docType;

    private String indexedDocumentIndex;
    private String indexedDocumentType;
    private String indexedDocumentId;
    private String indexedDocumentRouting;
    private String indexedDocumentPreference;
    private Long indexedDocumentVersion;
    private BytesReference documentSource;

    boolean indexedDocumentExists = true;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        queryField = randomAsciiOfLength(4);
        docType = randomAsciiOfLength(4);
        mapperService.merge("query_type", new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef("query_type",
                queryField, "type=percolator"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapperService.merge(docType, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(docType,
                STRING_FIELD_NAME, "type=text"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    @Override
    protected PercolateQueryBuilder doCreateTestQueryBuilder() {
        return doCreateTestQueryBuilder(randomBoolean());
    }

    private PercolateQueryBuilder doCreateTestQueryBuilder(boolean indexedDocument) {
        documentSource = randomSource();
        if (indexedDocument) {
            indexedDocumentIndex = randomAsciiOfLength(4);
            indexedDocumentType = randomAsciiOfLength(4);
            indexedDocumentId = randomAsciiOfLength(4);
            indexedDocumentRouting = randomAsciiOfLength(4);
            indexedDocumentPreference = randomAsciiOfLength(4);
            indexedDocumentVersion = (long) randomIntBetween(0, Integer.MAX_VALUE);
            return new PercolateQueryBuilder(queryField, docType, indexedDocumentIndex, indexedDocumentType, indexedDocumentId,
                    indexedDocumentRouting, indexedDocumentPreference, indexedDocumentVersion);
        } else {
            return new PercolateQueryBuilder(queryField, docType, documentSource);
        }
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
                    new GetResult(indexedDocumentIndex, indexedDocumentType, indexedDocumentId, 0L, true, documentSource,
                            Collections.emptyMap())
            );
        } else {
            return new GetResponse(
                    new GetResult(indexedDocumentIndex, indexedDocumentType, indexedDocumentId, -1, false, null, Collections.emptyMap())
            );
        }
    }

    @Override
    protected void doAssertLuceneQuery(PercolateQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, Matchers.instanceOf(PercolateQuery.class));
        PercolateQuery percolateQuery = (PercolateQuery) query;
        assertThat(percolateQuery.getDocumentType(), Matchers.equalTo(queryBuilder.getDocumentType()));
        assertThat(percolateQuery.getDocumentSource(), Matchers.equalTo(documentSource));
    }

    @Override
    public void testMustRewrite() throws IOException {
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> pqb.toQuery(createShardContext()));
        assertThat(e.getMessage(), equalTo("query builder must be rewritten first"));
        QueryBuilder rewrite = pqb.rewrite(createShardContext());
        PercolateQueryBuilder geoShapeQueryBuilder = new PercolateQueryBuilder(pqb.getField(), pqb.getDocumentType(), documentSource);
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testIndexedDocumentDoesNotExist() throws IOException {
        indexedDocumentExists = false;
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> pqb.rewrite(createShardContext()));
        String expectedString = "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentType + "/" +
                indexedDocumentId +  "] couldn't be found";
        assertThat(e.getMessage() , equalTo(expectedString));
    }

    // overwrite this test, because adding bogus field to the document part is valid and that would make the test fail
    // (the document part represents the document being percolated and any key value pair is allowed there)
    @Override
    public void testUnknownObjectException() throws IOException {
        String validQuery = createTestQueryBuilder().toString();
        int endPos = validQuery.indexOf("document");
        if (endPos == -1) {
            endPos = validQuery.length();
        }
        assertThat(validQuery, containsString("{"));
        for (int insertionPosition = 0; insertionPosition < endPos; insertionPosition++) {
            if (validQuery.charAt(insertionPosition) == '{') {
                String testQuery = validQuery.substring(0, insertionPosition) + "{ \"newField\" : " +
                        validQuery.substring(insertionPosition) + "}";
                try {
                    parseQuery(testQuery);
                    fail("some parsing exception expected for query: " + testQuery);
                } catch (ParsingException | Script.ScriptParseException | ElasticsearchParseException e) {
                    // different kinds of exception wordings depending on location
                    // of mutation, so no simple asserts possible here
                } catch (JsonParseException e) {
                    // mutation produced invalid json
                }
            }
        }
    }

    public void testRequiredParameters() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder(null, null, new BytesArray("{}"));
        });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> new PercolateQueryBuilder("_field", null, new BytesArray("{}")));
        assertThat(e.getMessage(), equalTo("[document_type] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> new PercolateQueryBuilder("_field", "_document_type", null));
        assertThat(e.getMessage(), equalTo("[document] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder(null, null, "_index", "_type", "_id", null, null, null);
        });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder("_field", null, "_index", "_type", "_id", null, null, null);
        });
        assertThat(e.getMessage(), equalTo("[document_type] is a required argument"));

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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parseQuery("{\"percolate\" : { \"document\": {}}"));
        assertThat(e.getMessage(), equalTo("[percolate] query is missing required [document_type] parameter"));
    }

    public void testCreateMultiDocumentSearcher() throws Exception {
        int numDocs = randomIntBetween(2, 8);
        List<ParseContext.Document> docs = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            docs.add(new ParseContext.Document());
        }

        Analyzer analyzer = new WhitespaceAnalyzer();
        ParsedDocument parsedDocument = new ParsedDocument(null, "_id", "_type", null, -1L, -1L, docs, null, null);
        IndexSearcher indexSearcher = PercolateQueryBuilder.createMultiDocumentSearcher(analyzer, parsedDocument);
        assertThat(indexSearcher.getIndexReader().numDocs(), equalTo(numDocs));

        // ensure that any query get modified so that the nested docs are never included as hits:
        Query query = new MatchAllDocsQuery();
        BooleanQuery result = (BooleanQuery) indexSearcher.createNormalizedWeight(query, true).getQuery();
        assertThat(result.clauses().size(), equalTo(2));
        assertThat(result.clauses().get(0).getQuery(), sameInstance(query));
        assertThat(result.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(result.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));
    }

    private static BytesReference randomSource() {
        try {
            XContentBuilder xContent = XContentFactory.jsonBuilder();
            xContent.map(RandomDocumentPicks.randomSource(random()));
            return xContent.bytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
