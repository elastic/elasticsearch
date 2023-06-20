/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponseTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.synonyms.SynonymsManagementAPIService.SynonymsReloadResult;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.synonyms.SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED;
import static org.elasticsearch.synonyms.SynonymsManagementAPIService.UpdateSynonymsResultStatus.UPDATED;

public class SynonymUpdateResponseSerializingTests extends AbstractWireSerializingTestCase<SynonymUpdateResponse> {

    @Override
    protected Writeable.Reader<SynonymUpdateResponse> instanceReader() {
        return SynonymUpdateResponse::new;
    }

    @Override
    protected SynonymUpdateResponse createTestInstance() {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesDetails = ReloadAnalyzersResponseTests
            .createRandomReloadDetails();
        ReloadAnalyzersResponse reloadAnalyzersResponse = new ReloadAnalyzersResponse(10, 10, 0, null, reloadedIndicesDetails);
        return new SynonymUpdateResponse(new SynonymsReloadResult<>(randomFrom(CREATED, UPDATED), reloadAnalyzersResponse));
    }

    @Override
    protected SynonymUpdateResponse mutateInstance(SynonymUpdateResponse instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testToXContent() throws IOException {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesNodes = Collections.singletonMap(
            "index",
            new ReloadAnalyzersResponse.ReloadDetails("index", Collections.singleton("nodeId"), Collections.singleton("my_analyzer"))
        );
        ReloadAnalyzersResponse reloadAnalyzersResponse = new ReloadAnalyzersResponse(10, 5, 0, null, reloadedIndicesNodes);
        SynonymUpdateResponse response = new SynonymUpdateResponse(new SynonymsReloadResult<>(CREATED, reloadAnalyzersResponse));

        String output = Strings.toString(response);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "result": "created",
              "reload_analyzers_details": {
                "_shards": {
                  "total": 10,
                  "successful": 5,
                  "failed": 0
                },
                "reload_details": [
                  {
                    "index": "index",
                    "reloaded_analyzers": [ "my_analyzer" ],
                    "reloaded_node_ids": [ "nodeId" ]
                  }
                ]
              }
            }"""), output);
    }
}
