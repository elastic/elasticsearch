/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.query;

import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link LanceKnnQuery}.
 */
public class LanceKnnQueryTests extends ESTestCase {

    public void testEqualsIdentical() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);

        assertThat(q1, equalTo(q2));
        assertThat(q1.hashCode(), equalTo(q2.hashCode()));
    }

    public void testEqualsDifferentField() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field1", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field2", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);

        assertThat(q1, not(equalTo(q2)));
    }

    public void testEqualsDifferentUri() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test1", vector, 10, 100, "cosine", null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field", "uri://test2", vector, 10, 100, "cosine", null, 3, null, null, null);

        assertThat(q1, not(equalTo(q2)));
    }

    public void testEqualsDifferentK() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field", "uri://test", vector, 20, 100, "cosine", null, 3, null, null, null);

        assertThat(q1, not(equalTo(q2)));
    }

    public void testEqualsDifferentNumCandidates() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field", "uri://test", vector, 10, 200, "cosine", null, 3, null, null, null);

        assertThat(q1, not(equalTo(q2)));
    }

    public void testEqualsDifferentSimilarity() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "dot_product", null, 3, null, null, null);

        assertThat(q1, not(equalTo(q2)));
    }

    public void testEqualsWithNullSimilarityDefaultsToCosine() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, null, null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);

        assertThat(q1, equalTo(q2));
        assertThat(q1.hashCode(), equalTo(q2.hashCode()));
    }

    public void testEqualsWithNull() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);

        assertFalse(q1.equals(null));
    }

    public void testEqualsWithDifferentClass() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);

        assertFalse(q1.equals("not a query"));
    }

    public void testToStringContainsFieldName() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery query = new LanceKnnQuery(
            "myField",
            "file:///path/to/data",
            vector,
            10,
            100,
            "cosine",
            null,
            3,
            null,
            null,
            null
        );

        String str = query.toString("ignored");
        assertThat(str, containsString("LanceKnnQuery"));
        assertThat(str, containsString("myField"));
        assertThat(str, containsString("file:///path/to/data"));
    }

    public void testVisitCallsVisitLeaf() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery query = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);

        final boolean[] visited = { false };
        QueryVisitor visitor = new QueryVisitor() {
            @Override
            public void visitLeaf(org.apache.lucene.search.Query query) {
                visited[0] = true;
            }
        };

        query.visit(visitor);
        assertTrue("visitLeaf should have been called", visited[0]);
    }

    public void testConstructorRequiresFieldName() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        expectThrows(
            NullPointerException.class,
            () -> new LanceKnnQuery(null, "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null)
        );
    }

    public void testConstructorRequiresStorageUri() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        expectThrows(
            NullPointerException.class,
            () -> new LanceKnnQuery("field", null, vector, 10, 100, "cosine", null, 3, null, null, null)
        );
    }

    public void testConstructorRequiresQueryVector() {
        expectThrows(
            NullPointerException.class,
            () -> new LanceKnnQuery("field", "uri://test", null, 10, 100, "cosine", null, 3, null, null, null)
        );
    }

    public void testHashCodeConsistent() {
        float[] vector = { 1.0f, 2.0f, 3.0f };
        LanceKnnQuery query = new LanceKnnQuery("field", "uri://test", vector, 10, 100, "cosine", null, 3, null, null, null);

        int hash1 = query.hashCode();
        int hash2 = query.hashCode();
        assertThat(hash1, equalTo(hash2));
    }

    public void testDifferentVectorsSameEquality() {
        // Note: The current equals implementation doesn't compare vectors, only field/uri/k/numCandidates/similarity
        // This test documents that behavior
        float[] vector1 = { 1.0f, 2.0f, 3.0f };
        float[] vector2 = { 4.0f, 5.0f, 6.0f };
        LanceKnnQuery q1 = new LanceKnnQuery("field", "uri://test", vector1, 10, 100, "cosine", null, 3, null, null, null);
        LanceKnnQuery q2 = new LanceKnnQuery("field", "uri://test", vector2, 10, 100, "cosine", null, 3, null, null, null);

        // These are equal because equals() doesn't compare vectors
        assertThat(q1, equalTo(q2));
    }
}
