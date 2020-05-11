/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class TransformTests extends ESTestCase {

    public void testNodeAttributes() {
        Settings.Builder builder = Settings.builder();
        boolean transformEnabled = randomBoolean();
        boolean transformPluginEnabled = randomBoolean();

        // randomly use explicit or default setting
        if ((transformEnabled && randomBoolean()) == false) {
            builder.put("node.transform", transformEnabled);
        }

        if (transformPluginEnabled == false) {
            builder.put("xpack.transform.enabled", transformPluginEnabled);
        }

        builder.put("node.attr.some_other_attrib", "value");
        Transform transform = createTransform(builder.build());
        assertNotNull(transform.additionalSettings());
        assertEquals(
            transformEnabled,
            Boolean.parseBoolean(transform.additionalSettings().get("node.attr.transform.node"))
        );
    }

    public void testNodeAttributesDirectlyGiven() {
        Settings.Builder builder = Settings.builder();
        builder.put("node.attr.transform.node", randomBoolean());

        Transform transform = createTransform(builder.build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, transform::additionalSettings);
        assertThat(
            e.getMessage(),
            equalTo("Directly setting transform node attributes is not permitted, please use the documented node settings instead")
        );
    }

    private Transform createTransform(Settings settings) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);

        return new Transform(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };
    }

}
