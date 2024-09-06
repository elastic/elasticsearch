/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;

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
     * Translate a {@code long} into a {@link NameId}, mapping the same {@code long}
     * into the same {@link NameId} each time. Each new {@code long} gets assigned
     * a unique id to the node, but when the same id is sent in the stream we get
     * the same result.
     */
    NameId mapNameId(long id) throws IOException;

    /**
     * Reads an Attribute using the attribute cache.
     * @param constructor the constructor needed to build the actual attribute when read from the wire
     * @return An attribute; this will generally be the same type as the provided constructor
     * @throws IOException
     */
    <A extends Attribute> A readAttributeWithCache(CheckedFunction<StreamInput, A, IOException> constructor) throws IOException;

    <A extends EsField> A readEsFieldWithCache() throws IOException;
}
