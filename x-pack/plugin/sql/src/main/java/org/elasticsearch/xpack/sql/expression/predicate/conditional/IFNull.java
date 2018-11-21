/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;

public class IFNull extends Coalesce {

    public IFNull(Location location, List<Expression> fields) {
        super(location, fields);

    }

    @Override
    protected NodeInfo<IFNull> info() {
        return NodeInfo.create(this, IFNull::new, children());
    }

    @Override
    protected Pipe makePipe() {
        if (children().size() != 2) {
            throw new SqlIllegalArgumentException("Line {}:{}: Unexpected number of arguments: expected [2], received[{}]",
                location().getLineNumber(), location().getColumnNumber(), children().size());
        }
        return super.makePipe();
    }
}
