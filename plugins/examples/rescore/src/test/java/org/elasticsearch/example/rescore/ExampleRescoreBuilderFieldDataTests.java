/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.example.rescore;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.test.AbstractBuilderTestCase;

import java.io.IOException;

public class ExampleRescoreBuilderFieldDataTests extends AbstractBuilderTestCase {

    //to test that the rescore plugin is able to pull data from the indexed documents
    //these following helper methods are called from the test below,
    //some helpful examples related to this are located circa feb 14 2024 at:
    //https://github.com/apache/lucene/blob/main/lucene/core/src/test/org/apache/lucene/search/TestQueryRescorer.java

    private String fieldFactorFieldName = "literalNameOfFieldUsedAsFactor";
    private float fieldFactorValue = 2.0f;

    private IndexSearcher getSearcher(IndexReader r) {
        IndexSearcher searcher = newSearcher(r);
        return searcher;
    }

    private IndexReader publishDocs(int numDocs, String fieldName, Directory dir) throws Exception {
        //here we populate a collection of documents into the mock search context
        //note they all have the same field factor value for convenience
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        for (int i = 0; i < numDocs; i++) {
            Document d = new Document();
            d.add(newStringField("id", Integer.toString(i), Field.Store.YES));
            d.add(new FloatField(fieldName, fieldFactorValue, Field.Store.YES ));
            w.addDocument(d);
        }
        IndexReader reader = w.getReader();
        w.close();
        return reader;
    }
    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {

        mapperService.merge(
            "_doc",
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(fieldFactorFieldName, "type=float"))),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }


    public void testRescoreUsingFieldData() throws Exception {
        //we want the originalScoreOfTopDocs to be lower than the rescored values
        //so that the order of the result has moved the rescored window to the top of the results
        float originalScoreOfTopDocs = 1.0f;

        //just like in the associated rescore builder factor testing
        //we will test a random factor on the incoming score docs
        //the division is just to leave room for whatever values are picked
        float factor = (float) randomDoubleBetween(1.0d, Float.MAX_VALUE/(fieldFactorValue * originalScoreOfTopDocs)-1, false);

        // Testing factorField specifically here for more example rescore debugging
        // setup a mock search context that will be able to locate fieldIndexData
        // provided from the index reader that follows

        Directory dir = newDirectory();
        //the rest of this test does not actually need more than 3 docs in the mock
        //however any number >= 3 is fine
        int numDocs = 3;
        IndexReader reader = publishDocs(numDocs, fieldFactorFieldName, dir);
        IndexSearcher searcher = getSearcher(reader);

        ExampleRescoreBuilder builder = new ExampleRescoreBuilder(factor, fieldFactorFieldName).windowSize(2);

        RescoreContext context = builder.buildContext(createSearchExecutionContext(searcher));

        //create and populate the TopDocs that will be provided to the rescore function
        TopDocs docs = new TopDocs(new TotalHits(10, TotalHits.Relation.EQUAL_TO), new ScoreDoc[3]);
        docs.scoreDocs[0] = new ScoreDoc(0, originalScoreOfTopDocs);
        docs.scoreDocs[1] = new ScoreDoc(1, originalScoreOfTopDocs);
        docs.scoreDocs[2] = new ScoreDoc(2, originalScoreOfTopDocs);
        context.rescorer().rescore(docs, searcher, context);

        //here we expect that windowSize docs have been re-scored, with remaining doc in the original state
        assertEquals(originalScoreOfTopDocs*factor*fieldFactorValue, docs.scoreDocs[0].score, 0.0f);
        assertEquals(originalScoreOfTopDocs*factor*fieldFactorValue, docs.scoreDocs[1].score, 0.0f);
        assertEquals(originalScoreOfTopDocs, docs.scoreDocs[2].score, 0.0f);

        //just to clean up the mocks
        reader.close();
        dir.close();
    }
}
