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

package org.elasticsearch.common.lucene.search.function;

import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.*;

/**
 * Test {@link RandomScoreFunction}
 */
public class RandomScoreFunctionTests extends ElasticsearchTestCase {

    private final String[] ids = { "1", "2", "3" };
    private IndexWriter writer;
    private AtomicReader reader;

    @After
    public void closeReaderAndWriterIfUsed() throws IOException {
        if (reader != null) {
            reader.close();
        }

        if (writer != null) {
            writer.close();
        }
    }

    /**
     * Create a "mock" {@link IndexSearcher} that uses an in-memory directory
     * containing three documents whose IDs are "1", "2", and "3" respectively.
     * @return Never {@code null}
     * @throws IOException if an unexpected error occurs while mocking
     */
    private IndexSearcher mockSearcher() throws IOException {
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        for (String id : ids) {
            Document document = new Document();
            document.add(new TextField("_id", id, Field.Store.YES));
            writer.addDocument(document);
        }
        reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(writer, true));
        return new IndexSearcher(reader);
    }

    /**
     * Given the same seed, the pseudo random number generator should match on
     * each use given the same number of invocations.
     */
    @Test
    public void testPrngNextFloatIsConsistent() {
        long seed = randomLong();

        RandomScoreFunction.PRNG prng = new RandomScoreFunction.PRNG(seed);
        RandomScoreFunction.PRNG prng2 = new RandomScoreFunction.PRNG(seed);

        // The seed will be changing the entire time, so each value should be
        //  different
        assertThat(prng.nextFloat(), equalTo(prng2.nextFloat()));
        assertThat(prng.nextFloat(), equalTo(prng2.nextFloat()));
        assertThat(prng.nextFloat(), equalTo(prng2.nextFloat()));
        assertThat(prng.nextFloat(), equalTo(prng2.nextFloat()));
    }

    @Test
    public void testPrngNextFloatSometimesFirstIsGreaterThanSecond() {
        boolean firstWasGreater = false;

        // Since the results themselves are intended to be random, we cannot
        //  just do @Repeat(iterations = 100) because some iterations are
        //  expected to fail
        for (int i = 0; i < 100; ++i) {
            long seed = randomLong();

            RandomScoreFunction.PRNG prng = new RandomScoreFunction.PRNG(seed);

            float firstRandom = prng.nextFloat();
            float secondRandom = prng.nextFloat();

            if (firstRandom > secondRandom) {
                firstWasGreater = true;
            }
        }

        assertTrue("First value was never greater than the second value", firstWasGreater);
    }

    @Test
    public void testPrngNextFloatSometimesFirstIsLessThanSecond() {
        boolean firstWasLess = false;

        // Since the results themselves are intended to be random, we cannot
        //  just do @Repeat(iterations = 100) because some iterations are
        //  expected to fail
        for (int i = 0; i < 1000; ++i) {
            long seed = randomLong();

            RandomScoreFunction.PRNG prng = new RandomScoreFunction.PRNG(seed);

            float firstRandom = prng.nextFloat();
            float secondRandom = prng.nextFloat();

            if (firstRandom < secondRandom) {
                firstWasLess = true;
            }
        }

        assertTrue("First value was never less than the second value", firstWasLess);
    }

    @Test
    public void testScorerResultsInRandomOrder() throws IOException {
        List<String> idsNotSpotted = Lists.newArrayList(ids);
        IndexSearcher searcher = mockSearcher();

        // Since the results themselves are intended to be random, we cannot
        //  just do @Repeat(iterations = 100) because some iterations are
        //  expected to fail
        for (int i = 0; i < 100; ++i) {
            // Randomly seeded to keep trying to shuffle without walking through
            //  values
            RandomScoreFunction function = new RandomScoreFunction(randomLong());
            // fulfilling contract
            function.setNextReader(reader.getContext());

            FunctionScoreQuery query = new FunctionScoreQuery(Queries.newMatchAllQuery(), function);

            // Testing that we get a random result
            TopDocs docs = searcher.search(query, 1);

            String id = reader.document(docs.scoreDocs[0].doc).getField("_id").stringValue();

            if (idsNotSpotted.remove(id) && idsNotSpotted.isEmpty()) {
                // short circuit test because we succeeded
                break;
            }
        }

        assertThat(idsNotSpotted, empty());
    }

    @Test
    public void testExplainScoreReportsOriginalSeed() {
        long seed = randomLong();
        Explanation subExplanation = new Explanation();

        RandomScoreFunction function = new RandomScoreFunction(seed);
        // Trigger a random call to change the seed to ensure that we are
        //  reporting the _original_ seed
        function.score(0, 1.0f);

        // Generate the randomScore explanation
        Explanation randomExplanation = function.explainScore(0, subExplanation);

        // Original seed should be there
        assertThat(randomExplanation.getDescription(), containsString("" + seed));
        assertThat(randomExplanation.getDetails(), arrayContaining(subExplanation));
    }


}
