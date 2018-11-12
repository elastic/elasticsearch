/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.jdbc.jdbcx.JdbcDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class JdbcMultiNodeIntegrationTestCase extends JdbcIntegrationTestCase {

    private List<Connection> setUpConnections() throws SQLException {
        String[] nodes = System.getProperty("tests.rest.cluster").split(",");
        List<Connection> connections = new ArrayList<>(nodes.length);
        for (String node : nodes) {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setUrl("jdbc:es://" + getProtocol() + "://" + node);
            dataSource.setProperties(connectionProperties());
            // create connections to each node in the cluster
            connections.add(dataSource.getConnection());
        }
        
        return connections;
    }
    
    @Override
    public Connection esJdbc() throws SQLException {
        List<Connection> connections = setUpConnections();
        // get a different node connection (hopefully) everytime
        return connections.get(randomIntBetween(0, connections.size() - 1));
    }
    
    public Map<String, Object> responseToMap(Response response) throws IOException {
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }
}
