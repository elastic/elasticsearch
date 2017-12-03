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

package org.elasticsearch.index.search;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ESToParentBlockJoinQueryTests extends ESTestCase {

    public void testEquals() {
        Query q1 = new ESToParentBlockJoinQuery(
                new TermQuery(new Term("is", "child")),
                new QueryBitSetProducer(new TermQuery(new Term("is", "parent"))),
                ScoreMode.Avg, "nested");

        Query q2 = new ESToParentBlockJoinQuery(
                new TermQuery(new Term("is", "child")),
                new QueryBitSetProducer(new TermQuery(new Term("is", "parent"))),
                ScoreMode.Avg, "nested");
        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());

        Query q3 = new ESToParentBlockJoinQuery(
                new TermQuery(new Term("is", "not_child")),
                new QueryBitSetProducer(new TermQuery(new Term("is", "parent"))),
                ScoreMode.Avg, "nested");
        assertFalse(q1.equals(q3));
        assertFalse(q1.hashCode() == q3.hashCode());

        Query q4 = new ESToParentBlockJoinQuery(
                new TermQuery(new Term("is", "child")),
                new QueryBitSetProducer(new TermQuery(new Term("is", "other_parent"))),
                ScoreMode.Avg, "nested");
        assertFalse(q1.equals(q4));
        assertFalse(q1.hashCode() == q4.hashCode());

        Query q5 = new ESToParentBlockJoinQuery(
                new TermQuery(new Term("is", "child")),
                new QueryBitSetProducer(new TermQuery(new Term("is", "parent"))),
                ScoreMode.Total, "nested");
        assertFalse(q1.equals(q5));
        assertFalse(q1.hashCode() == q5.hashCode());

        Query q6 = new ESToParentBlockJoinQuery(
                new TermQuery(new Term("is", "child")),
                new QueryBitSetProducer(new TermQuery(new Term("is", "parent"))),
                ScoreMode.Avg, "nested2");
        assertFalse(q1.equals(q6));
        assertFalse(q1.hashCode() == q6.hashCode());
    }

    public void testRewrite() throws IOException {
        Query q = new ESToParentBlockJoinQuery(
                new PhraseQuery("body", "term"), // rewrites to a TermQuery
                new QueryBitSetProducer(new TermQuery(new Term("is", "parent"))),
                ScoreMode.Avg, "nested");
        Query expected = new ESToParentBlockJoinQuery(
                 new TermQuery(new Term("body", "term")),
                 new QueryBitSetProducer(new TermQuery(new Term("is", "parent"))),
                 ScoreMode.Avg, "nested");
         Query rewritten = q.rewrite(new MultiReader());
         assertEquals(expected, rewritten);
    }
}
