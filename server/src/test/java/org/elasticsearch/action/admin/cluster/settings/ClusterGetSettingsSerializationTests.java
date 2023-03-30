/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ClusterGetSettingsSerializationTests extends AbstractWireSerializingTestCase<ClusterGetSettingsAction.Response> {
    @Override
    protected Writeable.Reader<ClusterGetSettingsAction.Response> instanceReader() {
        return ClusterGetSettingsAction.Response::new;
    }

    @Override
    protected ClusterGetSettingsAction.Response createTestInstance() {
        final Settings persistentSettings = Settings.builder()
            .put("persistent.foo.filtered", "bar")
            .put("persistent.foo.non_filtered", "baz")
            .build();

        final Settings transientSettings = Settings.builder()
            .put("transient.foo.filtered", "bar")
            .put("transient.foo.non_filtered", "baz")
            .build();

        final Settings allSettings = Settings.builder().put(persistentSettings).put(transientSettings).build();

        return new ClusterGetSettingsAction.Response(persistentSettings, transientSettings, allSettings);
    }

    @Override
    protected ClusterGetSettingsAction.Response mutateInstance(ClusterGetSettingsAction.Response instance) {
        final Settings otherSettings = Settings.builder().put("random.setting", randomAlphaOfLength(randomIntBetween(1, 12))).build();
        return switch (between(0, 2)) {
            case 0 -> new ClusterGetSettingsAction.Response(otherSettings, instance.transientSettings(), instance.settings());
            case 1 -> new ClusterGetSettingsAction.Response(instance.persistentSettings(), otherSettings, instance.settings());
            case 2 -> new ClusterGetSettingsAction.Response(instance.persistentSettings(), instance.transientSettings(), otherSettings);
            default -> throw new IllegalStateException("Unexpected switch value");
        };
    }
}
