/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.termenum.TermEnumRequest;
import org.elasticsearch.client.termenum.TermEnumResponse;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

public class TermEnumIT extends ESRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        // Create chain of doc IDs across indices 1->2->3
        Request doc1 = new Request(HttpPut.METHOD_NAME, "/index1/_doc/1");
        doc1.setJsonEntity("{ \"const\":\"aaa\"}");
        client().performRequest(doc1);

        Request doc2 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/2");
        doc2.setJsonEntity("{ \"const\":\"aba\"}");
        client().performRequest(doc2);

        Request doc3 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/3");
        doc3.setJsonEntity("{ \"const\":\"aba\"}");
        client().performRequest(doc3);
        
        Request doc4 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/4");
        doc4.setJsonEntity("{ \"const\":\"abc\"}");
        client().performRequest(doc4);

        Request doc5 = new Request(HttpPut.METHOD_NAME, "/index3/_doc/5");
        doc5.setJsonEntity("{ \"const\":\"def\"}");
        client().performRequest(doc5);


        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    public void testSuggests() throws Exception {
        checkResults("index*", "a", "aaa", "aba", "abc");
        checkResults("index1", "a", "aaa");
        checkResults("index2", "a", "aba", "abc");
        checkResults("index*", "ab", "aba", "abc");
        checkResults("index*", "d", "def");
        checkResults("index*", "z");
        checkResults("index*", "Ab", true, "aba", "abc");
    }

    public void testCaseInsensitive() throws Exception {
        checkResults("index*", "Ab", true, "aba", "abc");
    }

    public void checkResults(String indices, String prefix, String... expecteds) throws IOException {
        checkResults(indices, prefix, false, expecteds);
    }

    public void checkResults(String indices, String prefix, boolean caseInsensitive, String... expecteds) throws IOException {
        TermEnumRequest ter = new TermEnumRequest(indices);
        ter.field("const.keyword");
        ter.string(prefix);
        ter.caseInsensitive(caseInsensitive);
        TermEnumResponse result = highLevelClient().terms(ter, RequestOptions.DEFAULT);
        if (expecteds == null) {
            assertEquals(0, result.getTerms().size());
            return;
        }
        List<String> terms = result.getTerms();
        assertEquals(expecteds.length, terms.size());
        for (int i = 0; i < expecteds.length; i++) {
            assertEquals(expecteds[i], terms.get(i));
        }
    }
}
