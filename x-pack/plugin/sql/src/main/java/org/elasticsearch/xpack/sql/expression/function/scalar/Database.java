/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

public class Database extends BaseSystemFunction {

    public Database(Location location, Configuration configuration) {
        super(location, configuration);
    }

    @Override
    protected NodeInfo<Database> info() {
        return NodeInfo.create(this, Database::new, configuration());
    }
    
    @Override
    public Object fold() {
        return configuration().clusterName();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), configuration().clusterName());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(configuration().clusterName(), ((Database) obj).configuration().clusterName());
    }

}
