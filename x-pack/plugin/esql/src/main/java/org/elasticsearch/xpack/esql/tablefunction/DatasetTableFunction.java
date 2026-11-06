/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.parser.ParsingException;

/**
 * {@code DATASET(<data source>, {<dataset definition>})} — reads a dataset that was never registered,
 * against a data source that was.
 * <p>
 * The two arguments are the two halves a registered dataset is made of. The data source is named, and is
 * to supply the credentials and the endpoint, which a query cannot state and a data node binds from its
 * own cluster state. The payload is the rest of that dataset's definition — the same JSON the dataset API
 * carries, and the intent is for the same validator to check it, so that the two ways of defining a dataset
 * cannot drift and so that a credential cannot be written into a query text, that validator already
 * refusing a setting the data source holds as a secret.
 * <p>
 * <strong>None of that is implemented here.</strong> This class checks the shape of a call and nothing
 * more: no data source is looked up, no payload is validated, no privilege is evaluated, and nothing
 * rejects an argument that names a pattern rather than one data source. Each of those belongs to the
 * resolution step, which does not exist yet.
 */
public final class DatasetTableFunction implements TableFunction {

    public static final String NAME = "dataset";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void validate(TableFunctionCall call) {
        if (call.arguments().size() != 1) {
            throw new ParsingException(call.source(), "[{}] takes one data source name, found [{}]", call.name(), call.arguments().size());
        }
        if (call.payload() == null) {
            throw new ParsingException(call.source(), "[{}] requires a dataset definition", call.name());
        }
    }
}
