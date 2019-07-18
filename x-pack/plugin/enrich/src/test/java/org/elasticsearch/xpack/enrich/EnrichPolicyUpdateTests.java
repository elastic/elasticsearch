/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicyDefinition;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class EnrichPolicyUpdateTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEnrich.class);
    }

    public void testUpdatePolicyOnly() {
        IngestService ingestService = getInstanceFromNode(IngestService.class);
        EnrichProcessorFactory enrichProcessorFactory =
            (EnrichProcessorFactory) ingestService.getProcessorFactories().get(EnrichProcessorFactory.TYPE);

        EnrichPolicyDefinition instance1 =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of("index"), "key1", List.of("field1"));
        PutEnrichPolicyAction.Request putPolicyRequest = new PutEnrichPolicyAction.Request("my_policy", instance1);
        assertAcked(client().execute(PutEnrichPolicyAction.INSTANCE, putPolicyRequest).actionGet());
        assertThat(enrichProcessorFactory.policies.get("my_policy").getDefinition(), equalTo(instance1));

        String pipelineConfig = "{\"processors\":[{\"enrich\": {\"policy_name\": \"my_policy\", \"enrich_values\": []}}]}";
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("1", new BytesArray(pipelineConfig), XContentType.JSON);
        assertAcked(client().admin().cluster().putPipeline(putPipelineRequest).actionGet());
        Pipeline pipelineInstance1 = ingestService.getPipeline("1");
        assertThat(pipelineInstance1.getProcessors().get(0), instanceOf(ExactMatchProcessor.class));

        EnrichPolicyDefinition instance2 =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of("index"), "key2", List.of("field2"));
        ResourceAlreadyExistsException exc = expectThrows(ResourceAlreadyExistsException.class, () ->
            client().execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request("my_policy", instance2)).actionGet());
        assertTrue(exc.getMessage().contains("policy [my_policy] already exists"));
    }
}
