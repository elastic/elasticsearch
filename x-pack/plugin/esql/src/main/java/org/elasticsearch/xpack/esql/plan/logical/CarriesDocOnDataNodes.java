/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;

/**
 * Marks a command of several inputs that the {@code _doc} column of one input can pass through on the data nodes, each
 * output row keeping the {@code _doc} of the input row it comes from. Several output rows can keep the {@code _doc} of one
 * input row.
 * <p>
 * Commands of one input answer through {@link DocPreserving}. A {@code LOOKUP JOIN} isn't {@link DocPreserving}: it adds
 * the columns of another index to each row. But the {@code _doc} of its left rows passes through it, and the fetch phase
 * can load the other columns of those documents after the cut.
 * <p>
 * A command without either marker keeps the query loading its columns before the command, as it does without the fetch
 * phase, which is always correct. Add it only once a test shows that the fetch phase returns the same rows through the
 * command.
 */
public interface CarriesDocOnDataNodes {

    /**
     * The input whose {@code _doc} each output row keeps, or {@code null} when this command, as configured, doesn't let
     * {@code _doc} through.
     */
    @Nullable
    LogicalPlan docInput();
}
