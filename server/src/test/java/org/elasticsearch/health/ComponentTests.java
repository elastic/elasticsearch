/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasItems;

public class ComponentTests extends ESTestCase {

    public void testGroupIndicators() {

        var indicator1 = new HealthIndicator("indicator1", "component1", GREEN, null, null);
        var indicator2 = new HealthIndicator("indicator2", "component1", YELLOW, null, null);
        var indicator3 = new HealthIndicator("indicator3", "component2", GREEN, null, null);

        var components = Component.createComponentsFromIndicators(List.of(indicator1, indicator2, indicator3));

        assertThat(
            components,
            anyOf(
                hasItems(
                    new Component("component1", YELLOW, List.of(indicator2, indicator1)),
                    new Component("component2", GREEN, List.of(indicator3))
                ),
                hasItems(
                    new Component("component1", YELLOW, List.of(indicator1, indicator2)),
                    new Component("component2", GREEN, List.of(indicator3))
                )
            )
        );
    }
}
