/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcDriver;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DriverManagerRegistrationTests extends ESTestCase {


    public void testRegistration() throws Exception {
        String url = "jdbc:es:localhost:9200/";
        Driver driver = null;
        try {
            // can happen (if the driver jar was not loaded)
            driver = DriverManager.getDriver(url);
        } catch (SQLException ex) {
            assertEquals("No suitable driver", ex.getMessage());
        }
        boolean set = driver != null;
        try {
            Driver d = JdbcDriver.register();
            if (driver != null) {
                assertEquals(driver, d);
            }
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                // mimic DriverManager and unregister the driver
                JdbcDriver.deregister();
                return null;
            });

            SQLException ex = expectThrows(SQLException.class, () -> DriverManager.getDriver(url));
            assertEquals("No suitable driver", ex.getMessage());
        } finally {
            if (set) {
                JdbcDriver.register();
            }
        }
    }
}
