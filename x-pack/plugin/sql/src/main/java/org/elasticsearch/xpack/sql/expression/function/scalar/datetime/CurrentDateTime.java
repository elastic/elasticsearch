/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.function.scalar.ConfigurationFunction;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

public class CurrentDateTime extends ConfigurationFunction {

    public CurrentDateTime(Location location, Configuration configuration) {
        super(location, configuration, DataType.DATE);
    }

    @Override
    public Object fold() {
        return configuration().now();
    }

    @Override
    protected NodeInfo<CurrentDateTime> info() {
        return NodeInfo.create(this, CurrentDateTime::new, configuration());
    }
}
