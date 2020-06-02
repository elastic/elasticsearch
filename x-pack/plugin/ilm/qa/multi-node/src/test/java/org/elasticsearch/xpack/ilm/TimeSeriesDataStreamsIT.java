/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesDataStreamsIT extends ESRestTestCase {

    public void testRolloverAction() throws Exception {
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "hot", new RolloverAction(null, null, 1L));

        Settings lifecycleNameSetting = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName).build();
        Template template = new Template(lifecycleNameSetting, null, null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);

        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        assertBusy(() -> assertTrue(indexExists("logs-foo-000002")));
        assertBusy(() -> assertTrue(Boolean.parseBoolean((String) getIndexSettingsAsMap("logs-foo-000002").get("index.hidden"))));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), "logs-foo-000001"), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

}
