/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

/**
 * Interface for streams that can serialize plan components. This exists so
 * ESQL proper can expose streaming capability to ESQL-core. If the world is kind
 * and just we'll remove this when we flatten everything from ESQL-core into
 * ESQL proper.
 */
public interface PlanStreamInput {
    /**
     * The query sent by the user to build this plan. This is used to rebuild
     * {@link Source} without sending the query over the wire over and over
     * and over again.
     */
    String sourceText();

    /**
     * Read a {@link NameId} from the stream. Name-id have to be consistently
     * mapped during serialization but need to be globally unique. This handles
     * that combination of constraints.
     */
    NameId readNameId() throws IOException;
}
