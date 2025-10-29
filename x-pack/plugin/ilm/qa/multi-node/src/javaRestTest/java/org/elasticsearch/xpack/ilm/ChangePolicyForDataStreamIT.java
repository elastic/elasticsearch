/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.IlmESRestTestCase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.hamcrest.Matchers.equalTo;

public class ChangePolicyForDataStreamIT extends IlmESRestTestCase {

    /**
     * This test verifies that after changing the lifecycle policy for a data stream,
     * ILM will use the updated phase definition from new policy for the current phase (if compatible).
     * <p>
     * The test creates two policies:
     * - the first with a hot phase requiring 1000 documents to rollover
     * - the second with a hot phase requiring 2 documents to rollover
     * <p>
     * A data stream is created with the first policy and the test ensures it is on the rollover step.
     * The data stream policy is then changed to the second policy.
     * A document is indexed which should trigger the rollover given it should now use rollover criteria from second policy.
     */
    public void testChangePolicyForDataStream() throws Exception {
        final String oldPolicy = "old_policy";
        final String newPolicy = "new_policy";
        final String dataStream = "logs-ds";
        final String template = "template-ds";

        // old_policy: hot rollover=1000
        createNewSingletonPolicy(
            client(),
            oldPolicy,
            "hot",
            new RolloverAction(null, null, null, 1000L, null, null, null, null, null, null)
        );
        // new_policy: hot rollover=2
        createNewSingletonPolicy(client(), newPolicy, "hot", new RolloverAction(null, null, null, 2L, null, null, null, null, null, null));

        // data stream has old policy
        createComposableTemplateWithPolicy(template, dataStream + "*", oldPolicy);

        // create DS
        indexDocument(client(), dataStream, true);

        // wait until the only backing index is moved to wait-for-rollover-ready by ILM
        assertBusy(() -> {
            final List<String> backing = getDataStreamBackingIndexNames(dataStream);
            assertThat(backing.size(), equalTo(1));
            final String writeIdx = backing.getFirst();
            assertThat(getStepKeyForIndex(client(), writeIdx), equalTo(new StepKey("hot", RolloverAction.NAME, "check-rollover-ready")));
        }, 30, TimeUnit.SECONDS);

        // swap DS policy to new_policy via data stream settings
        updateDataStreamPolicy(dataStream, newPolicy);

        // confirm `_ilm/explain` reflects new_policy on the current write index
        {
            final String writeIdx = getDataStreamBackingIndexNames(dataStream).getFirst();
            final var explain = explainIndex(client(), writeIdx);
            assertThat(explain.get("phase"), equalTo("hot"));
            @SuppressWarnings("unchecked")
            var phaseExec = (java.util.Map<String, Object>) explain.get("phase_execution");
            assertNotNull(phaseExec);
            assertThat(phaseExec.get("policy"), equalTo(newPolicy));
        }

        // index document to trigger rollover criteria on new policy
        indexDocument(client(), dataStream, true);

        // assert ILM performed rollover and original index completed hot
        assertBusy(() -> {
            final List<String> backing = getDataStreamBackingIndexNames(dataStream);
            assertThat(backing.size(), equalTo(2));
            final String original = backing.getFirst();
            assertThat(getStepKeyForIndex(client(), original), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * This test verifies that if we update a data stream lifecycle policy, to a policy where we can't refresh the cached steps
     * (in this case, we replace a step), that the cached step definition is kept and used for the index until it moves to the next phase.
     */
    public void testDataStreamHonoursCachedPhaseAfterPolicyUpdate() throws Exception {
        final String oldPolicy = "old_policy";
        final String newPolicy = "new_policy";
        final String dataStream = "logs-ds";
        final String template = "template-ds";

        // start with rollover=2
        createNewSingletonPolicy(client(), oldPolicy, "hot", new RolloverAction(null, null, null, 2L, null, null, null, null, null, null));
        // no rollover, will be incompatible with current step
        createNewSingletonPolicy(client(), newPolicy, "hot", new SetPriorityAction(200));

        // create DS with policy
        createComposableTemplateWithPolicy(template, dataStream + "*", oldPolicy);
        indexDocument(client(), dataStream, true);

        // wait until the only backing index is moved to wait-for-rollover-ready by ILM
        assertBusy(() -> {
            final List<String> backing = getDataStreamBackingIndexNames(dataStream);
            assertThat(backing.size(), equalTo(1));
            final String writeIdx = backing.getFirst();
            assertThat(getStepKeyForIndex(client(), writeIdx), equalTo(new StepKey("hot", RolloverAction.NAME, "check-rollover-ready")));
        }, 30, TimeUnit.SECONDS);

        // swap the DS to a policy that has no rollover (incompatible current step), so cached step is kept
        updateDataStreamPolicy(dataStream, newPolicy);

        // confirm `_ilm/explain` reflects old_policy within the phase execution, i.e. cached step is kept
        {
            final String writeIdx = getDataStreamBackingIndexNames(dataStream).getFirst();
            final var explain = explainIndex(client(), writeIdx);
            assertThat(explain.get("phase"), equalTo("hot"));
            @SuppressWarnings("unchecked")
            var phaseExec = (java.util.Map<String, Object>) explain.get("phase_execution");
            assertNotNull(phaseExec);
            assertThat(phaseExec.get("policy"), equalTo(oldPolicy));
        }

        // index another document to trigger rollover from cached step
        indexDocument(client(), dataStream, true);

        // verify first index rolled over due to new policy and has finished hot phase,
        // and new index uses new policy too, and has finished hot phase since no rollover step
        assertBusy(() -> {
            final List<String> backing = getDataStreamBackingIndexNames(dataStream);
            assertThat(backing.size(), equalTo(2));
            final String previousIndex = backing.getFirst();
            final String newIndex = backing.getLast();
            assertThat(getStepKeyForIndex(client(), previousIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
            assertThat(getStepKeyForIndex(client(), newIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
        }, 30, TimeUnit.SECONDS);
    }

    private void updateDataStreamPolicy(final String dataStreamName, final String policyName) throws Exception {
        final Request req = new Request("PUT", "/_data_stream/" + dataStreamName + "/_settings");
        req.setEntity(new StringEntity("{ \"index.lifecycle.name\": \"" + policyName + "\" }", ContentType.APPLICATION_JSON));
        assertOK(client().performRequest(req));
    }

    private void createComposableTemplateWithPolicy(final String templateName, final String indexPattern, final String policyName)
        throws Exception {
        final String body = Strings.format("""
            {
              "index_patterns": "%s",
              "data_stream": {},
              "template": {
                "settings": {
                  "index.lifecycle.name": "%s"
                }
              }
            }
            """, indexPattern, policyName);
        final Request req = new Request("PUT", "_index_template/" + templateName);
        req.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        assertOK(client().performRequest(req));
    }
}
