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
        boolean remoteEnabled = randomBoolean();

        // randomly use explicit or default setting
        if ((transformEnabled && randomBoolean()) == false) {
            builder.put("node.transform", transformEnabled);
        }

        // randomly use explicit or default setting
        if ((remoteEnabled && randomBoolean()) == false) {
            builder.put("cluster.remote.connect", remoteEnabled);
        }

        if (transformPluginEnabled == false) {
            builder.put("xpack.transform.enabled", transformPluginEnabled);
        }

        builder.put("node.attr.some_other_attrib", "value");
        Transform transform = createTransform(builder.build());
        assertNotNull(transform.additionalSettings());
        assertEquals(
            transformPluginEnabled && transformEnabled,
            Boolean.parseBoolean(transform.additionalSettings().get("node.attr.transform.node"))
        );
        assertEquals(
            transformPluginEnabled && remoteEnabled,
            Boolean.parseBoolean(transform.additionalSettings().get("node.attr.transform.remote_connect"))
        );
    }

    public void testNodeAttributesDirectlyGiven() {
        Settings.Builder builder = Settings.builder();

        if (randomBoolean()) {
            builder.put("node.attr.transform.node", randomBoolean());
        } else {
            builder.put("node.attr.transform.remote_connect", randomBoolean());
        }

        Transform transform = createTransform(builder.build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> transform.additionalSettings());
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
