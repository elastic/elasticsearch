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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class DeleteSynonymRuleActionResponseSerializingTests extends AbstractWireSerializingTestCase<DeleteSynonymRuleAction.Response> {

    @Override
    protected Writeable.Reader<DeleteSynonymRuleAction.Response> instanceReader() {
        return DeleteSynonymRuleAction.Response::new;
    }

    @Override
    protected DeleteSynonymRuleAction.Response createTestInstance() {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesDetails = ReloadAnalyzersResponseTests
            .createRandomReloadDetails();
        AcknowledgedResponse acknowledgedResponse = AcknowledgedResponse.of(randomBoolean());
        return new DeleteSynonymRuleAction.Response(
            acknowledgedResponse,
            new ReloadAnalyzersResponse(10, 10, 0, null, reloadedIndicesDetails)
        );
    }

    @Override
    protected DeleteSynonymRuleAction.Response mutateInstance(DeleteSynonymRuleAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testToXContent() throws IOException {
        Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesNodes = Collections.singletonMap(
            "index",
            new ReloadAnalyzersResponse.ReloadDetails("index", Collections.singleton("nodeId"), Collections.singleton("my_analyzer"))
        );
        ReloadAnalyzersResponse reloadAnalyzersResponse = new ReloadAnalyzersResponse(10, 5, 0, null, reloadedIndicesNodes);
        AcknowledgedResponse acknowledgedResponse = AcknowledgedResponse.of(true);
        DeleteSynonymRuleAction.Response response = new DeleteSynonymRuleAction.Response(acknowledgedResponse, reloadAnalyzersResponse);

        String output = Strings.toString(response);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "acknowledged": true,
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
