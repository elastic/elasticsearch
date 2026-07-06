/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexFrozenTransition;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FrozenTransitionInfoProviderTests extends ESTestCase {

    public void testNoop() {
        FrozenTransitionInfoProvider provider = FrozenTransitionInfoProvider.noop();
        assertThat(provider.infoAvailable(), is(false));
        assertThat(
            provider.getTransitionStatus(randomProjectIdOrDefault(), randomAlphaOfLength(10)),
            equalTo(ExplainIndexFrozenTransition.Status.NOT_AVAILABLE)
        );
    }
}
