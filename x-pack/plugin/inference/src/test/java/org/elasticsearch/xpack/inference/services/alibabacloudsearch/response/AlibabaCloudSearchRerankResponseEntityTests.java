/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AlibabaCloudSearchRerankResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        InferenceServiceResults parsedResults = AlibabaCloudSearchRerankResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseLiteral.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        List<RankedDocsResults.RankedDoc> expected = responseLiteralDocs();
        for (int i = 0; i < ((RankedDocsResults) parsedResults).getRankedDocs().size(); i++) {
            assertThat(((RankedDocsResults) parsedResults).getRankedDocs().get(i).index(), is(expected.get(i).index()));
        }
    }

    private final String responseLiteral = """
        {
          "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
          "latency": 564.903929,
          "usage": {
            "doc_count": 2
          },
          "result": {
           "scores":[
             {
               "index":1,
               "score": 1.37
             },
             {
               "index":0,
               "score": -0.3
             }
           ]
          }
        }
        """;

    private ArrayList<RankedDocsResults.RankedDoc> responseLiteralDocs() {
        var list = new ArrayList<RankedDocsResults.RankedDoc>();

        list.add(new RankedDocsResults.RankedDoc(1, 1.37F, null));
        list.add(new RankedDocsResults.RankedDoc(0, -0.3F, null));
        return list;
    };
}
