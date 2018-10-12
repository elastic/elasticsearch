/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.qa.sql.nosecurity;

import org.elasticsearch.xpack.qa.sql.jdbc.ResultSetTestCase;

/*
 * Integration testing class for "no security" (cluster running without the Security plugin,
 * or the Security is disbled) scenario. Runs all tests in the base class.
 */
public class JdbcResultSetIT extends ResultSetTestCase {
}
