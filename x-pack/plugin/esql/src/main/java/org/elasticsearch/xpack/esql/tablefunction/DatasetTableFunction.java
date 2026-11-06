/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * {@code DATASET(<data source>, {<dataset definition>})} — reads a dataset that was never registered,
 * against a data source that was.
 * <p>
 * The two arguments are the two halves a registered dataset is made of. The data source is named, and
 * supplies the credentials and the endpoint, which a query cannot state and a data node binds from its
 * own cluster state. The payload is the rest of that dataset's definition — the same JSON the dataset
 * API carries, validated by the same validator. That shared validator is what keeps the two ways of
 * defining a dataset from drifting, and what keeps a credential out of a query text, since it already
 * refuses a setting the data source holds as a secret.
 * <p>
 * Exactly one data source, never a pattern: a data source is the unit a privilege is granted over, so a
 * call that could name several would be a call whose reach is not what was authorized.
 */
public final class DatasetTableFunction implements TableFunction {

    public static final String NAME = "dataset";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public LogicalPlan toRelation(TableFunctionCall call) {
        if (call.arguments().size() != 1) {
            throw new ParsingException(call.source(), "[{}] takes one data source name, found [{}]", call.name(), call.arguments().size());
        }
        if (call.payload() == null) {
            throw new ParsingException(call.source(), "[{}] requires a dataset definition", call.name());
        }
        // Whether the name denotes one local data source rather than a pattern or a remote reference is
        // checked where the name is resolved, beside the authorization that has to agree with it, rather
        // than by a second copy of that test here. That resolution is not written yet, so until it is, a
        // well-formed call is recognized and refused rather than planned.
        throw new ParsingException(call.source(), "[{}] is not supported yet", call.name());
    }
}
