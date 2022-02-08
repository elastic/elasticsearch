/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;

public class HealthComponentResultTests extends ESTestCase {

    public void testGroupIndicators() {

        // var indicator1 = new HealthIndicatorResult("indicator1", "component1", GREEN, null, null);
        // var indicator2 = new HealthIndicatorResult("indicator2", "component1", YELLOW, null, null);
        // var indicator3 = new HealthIndicatorResult("indicator3", "component2", GREEN, null, null);
        //
        // var components = HealthComponentResult.createComponentsFromIndicators(List.of(indicator1, indicator2, indicator3));
        //
        // assertThat(
        // components,
        // anyOf(
        // hasItems(
        // new HealthComponentResult("component1", YELLOW, List.of(indicator2, indicator1)),
        // new HealthComponentResult("component2", GREEN, List.of(indicator3))
        // ),
        // hasItems(
        // new HealthComponentResult("component1", YELLOW, List.of(indicator1, indicator2)),
        // new HealthComponentResult("component2", GREEN, List.of(indicator3))
        // )
        // )
        // );
    }
}
