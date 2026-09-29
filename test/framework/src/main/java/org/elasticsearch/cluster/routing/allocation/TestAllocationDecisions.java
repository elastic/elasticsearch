/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

/**
 * Some constants to use in tests when we need a {@link Decision.Type#NO} or {@link Decision.Type#NOT_PREFERRED}
 * decision.
 * <p>
 * {@link Decision#NO} and {@link Decision#NOT_PREFERRED} do not define a label, and {@link AllocationDeciders}
 * asserts that any time a NO or NOT_PREFERRED decision is returned it should have a label. We don't want to
 * add labels to {@link Decision#NO} and {@link Decision#NOT_PREFERRED} because we don't want people to use them
 * they should create their own {@link Decision} instances with accurate label values populated.
 */
public class TestAllocationDecisions {

    public static Decision NO_DECISION = new Decision.Single(Decision.Type.NO, "test_decider", null);
    public static Decision NOT_PREFERRED_DECISION = new Decision.Single(Decision.Type.NOT_PREFERRED, "test_decider", null);
}
