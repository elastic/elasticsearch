/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class NodeMetadataVersionTests extends ESTestCase {

    public void testVersionComparison() {
        NodeMetadataVersion V_7_17_0 = NodeMetadataVersion.V_7_17_0;
        NodeMetadataVersion V_8_0_0 = NodeMetadataVersion.V_8_0_0;
        assertThat(V_7_17_0.before(V_8_0_0), is(true));
        assertThat(V_7_17_0.before(V_7_17_0), is(false));
        assertThat(V_8_0_0.before(V_7_17_0), is(false));

        assertThat(V_7_17_0.onOrBefore(V_8_0_0), is(true));
        assertThat(V_7_17_0.onOrBefore(V_7_17_0), is(true));
        assertThat(V_8_0_0.onOrBefore(V_7_17_0), is(false));

        assertThat(V_7_17_0.after(V_8_0_0), is(false));
        assertThat(V_7_17_0.after(V_7_17_0), is(false));
        assertThat(V_8_0_0.after(V_7_17_0), is(true));

        assertThat(V_7_17_0.onOrAfter(V_8_0_0), is(false));
        assertThat(V_7_17_0.onOrAfter(V_7_17_0), is(true));
        assertThat(V_8_0_0.onOrAfter(V_7_17_0), is(true));

        assertThat(V_7_17_0, is(lessThan(V_8_0_0)));
        assertThat(V_7_17_0.compareTo(V_7_17_0), is(0));
        assertThat(V_8_0_0, is(greaterThan(V_7_17_0)));
    }

}
