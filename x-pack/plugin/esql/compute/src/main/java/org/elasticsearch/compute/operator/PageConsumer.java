/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.function.Consumer;

/**
 * A {@link OutputOperator} consumer that sends its pages somewhere, and so takes part in the driver's lifecycle: it
 * can hold the driver back while work is in flight, and it can still be finishing after the last page.
 */
public interface PageConsumer extends Consumer<Page>, Releasable {

    /**
     * Whether the driver should wait before handing over another page, and a listener that completes when it may
     * continue. Returning {@link Operator#NOT_BLOCKED} means carry on.
     */
    IsBlockedResult isBlocked();

    /** Called once no more pages will arrive. Work already handed over may still be outstanding. */
    void finish();

    /** Whether every page handed over has been dealt with, so the query may complete. */
    boolean isFinished();
}
