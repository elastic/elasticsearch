/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.common.CheckedSupplier;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractJdbcConnectionSource extends ExternalResource implements CheckedSupplier<Connection, SQLException> {

}
