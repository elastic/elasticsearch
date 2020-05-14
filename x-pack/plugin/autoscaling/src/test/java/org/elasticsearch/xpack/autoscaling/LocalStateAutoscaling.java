/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Collection;
import java.util.stream.Collectors;

public class LocalStateAutoscaling extends LocalStateCompositeXPackPlugin implements ExtensiblePlugin, AutoscalingPlugin {

    public LocalStateAutoscaling(final Settings settings) {
        super(settings, null);
        plugins.add(new Autoscaling(settings));
    }

    @Override
    public Collection<AutoscalingDeciderService<? extends AutoscalingDecider>> deciders() {
        return filterPlugins(AutoscalingPlugin.class).stream().flatMap(p -> p.deciders().stream()).collect(Collectors.toList());
    }
}
