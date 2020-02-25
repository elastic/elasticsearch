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
        boolean transformEnabled = true;
        boolean remoteEnabled = true;

        if (randomBoolean()) {
            transformEnabled = randomBoolean();
            if (randomBoolean()) {
                builder.put("node.transform", transformEnabled);
                if (randomBoolean()) {
                    // note: the case where node.transform: true and xpack.transform.enabled: false is benign
                    builder.put("xpack.transform.enabled", randomBoolean());
                }
            } else {
                builder.put("xpack.transform.enabled", transformEnabled);
            }
        }

        if (randomBoolean()) {
            remoteEnabled = randomBoolean();
            builder.put("cluster.remote.connect", remoteEnabled);
        }

        builder.put("node.attr.some_other_attrib", "value");
        Transform transform = createTransform(builder.build());
        assertNotNull(transform.additionalSettings());
        assertEquals(transformEnabled, Boolean.parseBoolean(transform.additionalSettings().get("node.attr.transform.node")));
        assertEquals(remoteEnabled, Boolean.parseBoolean(transform.additionalSettings().get("node.attr.transform.remote_connect")));
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
