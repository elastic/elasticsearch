/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.enrich.AbstractEnrichTestCase.createSourceIndices;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class EnrichPolicyUpdateTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEnrich.class, ReindexPlugin.class, IngestCommonPlugin.class);
    }

    public void testUpdatePolicyOnly() {
        IngestService ingestService = getInstanceFromNode(IngestService.class);
        createIndex("index", Settings.EMPTY, "_doc", "key1", "type=keyword", "field1", "type=keyword");

        EnrichPolicy instance1 = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("index"), "key1", List.of("field1"));
        createSourceIndices(client(), instance1);
        PutEnrichPolicyAction.Request putPolicyRequest = new PutEnrichPolicyAction.Request("my_policy", instance1);
        assertAcked(client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet());
        assertThat(
            "Execute failed",
            client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request("my_policy"))
                .actionGet()
                .getStatus()
                .isCompleted(),
            equalTo(true)
        );

        String pipelineConfig =
            "{\"processors\":[{\"enrich\": {\"policy_name\": \"my_policy\", \"field\": \"key\", \"target_field\": \"target\"}}]}";
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("1", new BytesArray(pipelineConfig), XContentType.JSON);
        assertAcked(client().admin().cluster().putPipeline(putPipelineRequest).actionGet());
        Pipeline pipelineInstance1 = ingestService.getPipeline("1");
        assertThat(pipelineInstance1.getProcessors().get(0), instanceOf(MatchProcessor.class));

        EnrichPolicy instance2 = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("index2"), "key2", List.of("field2"));
        createSourceIndices(client(), instance2);
        ResourceAlreadyExistsException exc = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> client().execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request("my_policy", instance2)).actionGet()
        );
        assertTrue(exc.getMessage().contains("policy [my_policy] already exists"));
    }
}
