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

public class User extends BaseSystemFunction {

    public User(Location location, Configuration configuration) {
        super(location, configuration);
    }

    @Override
    protected NodeInfo<User> info() {
        return NodeInfo.create(this, User::new, configuration());
    }

    @Override
    public boolean nullable() {
        return true;
    }

    @Override
    public Object fold() {
        return configuration().username();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), configuration().username());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(configuration().username(), ((User) obj).configuration().username());
    }

}
