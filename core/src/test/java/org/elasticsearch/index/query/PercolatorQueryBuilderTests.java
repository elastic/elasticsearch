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

package org.elasticsearch.index.query;

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.script.Script;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PercolatorQueryBuilderTests extends AbstractQueryTestCase<PercolatorQueryBuilder> {

    private String indexedDocumentIndex;
    private String indexedDocumentType;
    private String indexedDocumentId;
    private String indexedDocumentRouting;
    private String indexedDocumentPreference;
    private Long indexedDocumentVersion;
    private BytesReference documentSource;

    boolean indexedDocumentExists = true;

    @Override
    protected PercolatorQueryBuilder doCreateTestQueryBuilder() {
        return doCreateTestQueryBuilder(randomBoolean());
    }

    private PercolatorQueryBuilder doCreateTestQueryBuilder(boolean indexedDocument) {
        String docType = randomAsciiOfLength(4);
        documentSource = randomSource();
        if (indexedDocument) {
            indexedDocumentIndex = randomAsciiOfLength(4);
            indexedDocumentType = randomAsciiOfLength(4);
            indexedDocumentId = randomAsciiOfLength(4);
            indexedDocumentRouting = randomAsciiOfLength(4);
            indexedDocumentPreference = randomAsciiOfLength(4);
            indexedDocumentVersion = (long) randomIntBetween(0, Integer.MAX_VALUE);
            return new PercolatorQueryBuilder(docType, indexedDocumentIndex, indexedDocumentType, indexedDocumentId,
                    indexedDocumentRouting, indexedDocumentPreference, indexedDocumentVersion);
        } else {
            return new PercolatorQueryBuilder(docType, documentSource);
        }
    }

    /**
     * prevent fields in the "document" field from being shuffled randomly, because it later is parsed to
     * a {@link BytesReference} and even though the documents are the same, equals will fail when comparing
     * BytesReference
     */
    @Override
    protected Set<String> provideShuffleproofFields() {
        Set<String> fieldNames = new HashSet<>();
        fieldNames.add(PercolatorQueryParser.DOCUMENT_FIELD.getPreferredName());
        return fieldNames;
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
    protected void doAssertLuceneQuery(PercolatorQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, Matchers.instanceOf(PercolatorQuery.class));
        PercolatorQuery percolatorQuery = (PercolatorQuery) query;
        assertThat(percolatorQuery.getDocumentType(), Matchers.equalTo(queryBuilder.getDocumentType()));
        assertThat(percolatorQuery.getDocumentSource(), Matchers.equalTo(documentSource));
    }

    @Override
    public void testMustRewrite() throws IOException {
        PercolatorQueryBuilder pqb = doCreateTestQueryBuilder(true);
        try {
            pqb.toQuery(queryShardContext());
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("query builder must be rewritten first"));
        }
        QueryBuilder<?> rewrite = pqb.rewrite(queryShardContext());
        PercolatorQueryBuilder geoShapeQueryBuilder = new PercolatorQueryBuilder(pqb.getDocumentType(), documentSource);
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testIndexedDocumentDoesNotExist() throws IOException {
        indexedDocumentExists = false;
        PercolatorQueryBuilder pqb = doCreateTestQueryBuilder(true);
        try {
            pqb.rewrite(queryShardContext());
            fail("ResourceNotFoundException expected");
        } catch (ResourceNotFoundException e) {
            String expectedString = "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentType + "/" +
                    indexedDocumentId +  "] couldn't be found";
            assertThat(e.getMessage() , equalTo(expectedString));
        }
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
        try {
            QueryBuilders.percolatorQuery(null, new BytesArray("{}"));
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[document_type] is a required argument"));
        }
        try {
            QueryBuilders.percolatorQuery("_document_type", null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[document] is a required argument"));
        }
        try {
            QueryBuilders.percolatorQuery(null, "_index", "_type", "_id", null, null, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[document_type] is a required argument"));
        }
        try {
            QueryBuilders.percolatorQuery("_document_type", null, "_type", "_id", null, null, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[index] is a required argument"));
        }
        try {
            QueryBuilders.percolatorQuery("_document_type", "_index", null, "_id", null, null, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[type] is a required argument"));
        }
        try {
            QueryBuilders.percolatorQuery("_document_type", "_index", "_type", null, null, null, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[id] is a required argument"));
        }
    }

    public void testFromJsonNoDocumentType() throws IOException {
        try {
            parseQuery("{\"percolator\" : { \"document\": {}}");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[percolator] query is missing required [document_type] parameter"));
        }
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
