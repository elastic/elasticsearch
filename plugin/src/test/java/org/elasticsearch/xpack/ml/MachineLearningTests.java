/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class MachineLearningTests extends ESTestCase {

    public void testNoAttributes_givenNoClash() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            builder.put("xpack.ml.max_open_jobs", randomIntBetween(9, 12));
        }
        builder.put("node.attr.foo", "abc");
        builder.put("node.attr.ml.bar", "def");
        MachineLearning machineLearning = createMachineLearning(builder.build());
        assertNotNull(machineLearning.additionalSettings());
    }

    public void testNoAttributes_givenSameAndMlEnabled() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", true);
            builder.put("node.attr.ml.enabled", true);
        }
        if (randomBoolean()) {
            int maxOpenJobs = randomIntBetween(5, 15);
            builder.put("xpack.ml.max_open_jobs", maxOpenJobs);
            builder.put("node.attr.ml.max_open_jobs", maxOpenJobs);
        }
        MachineLearning machineLearning = createMachineLearning(builder.build());
        assertNotNull(machineLearning.additionalSettings());
    }

    public void testNoAttributes_givenClash() {
        Settings.Builder builder = Settings.builder();
        boolean enabled = true;
        if (randomBoolean()) {
            enabled = randomBoolean();
            builder.put("xpack.ml.enabled", enabled);
        }
        if (randomBoolean()) {
            builder.put("xpack.ml.max_open_jobs", randomIntBetween(9, 12));
        }
        if (randomBoolean()) {
            builder.put("node.attr.ml.enabled", !enabled);
        } else {
            builder.put("node.attr.ml.max_open_jobs", randomIntBetween(13, 15));
        }
        MachineLearning machineLearning = createMachineLearning(builder.build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, machineLearning::additionalSettings);
        assertThat(e.getMessage(), startsWith("Directly setting [node.attr.ml."));
        assertThat(e.getMessage(), containsString("] is not permitted - " +
                "it is reserved for machine learning. If your intention was to customize machine learning, set the [xpack.ml."));
    }

    private MachineLearning createMachineLearning(Settings settings) {
        return new MachineLearning(settings, mock(Environment.class), mock(XPackLicenseState.class));
    }
}
