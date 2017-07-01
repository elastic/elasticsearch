/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// poor's man JdbcTemplate
public class JdbcTemplate {
    private final CheckedSupplier<Connection, SQLException> conn;

    public JdbcTemplate(CheckedSupplier<Connection, SQLException> conn) {
        this.conn = conn;
    }

    public void consume(CheckedConsumer<Connection, SQLException> c) throws SQLException {
        try (Connection con = conn.get()) {
            c.accept(con);
        }
    }

    public <T> T map(CheckedFunction<Connection, T, SQLException> c) throws SQLException {
        try (Connection con = conn.get()) {
            return c.apply(con);
        }
    }

    public <T> T query(String q, CheckedFunction<ResultSet, T, SQLException> f) throws SQLException {
        return map(c -> {
            try (Statement st = c.createStatement();
                 ResultSet rset = st.executeQuery(q)) {
                return f.apply(rset);
            }
        });
    }

    public <T> T queryObject(String q, Class<T> type) throws SQLException {
        return query(q, singleResult(type));
    }

    public void execute(String query) throws Exception {
        map(c -> {
            try (Statement st = c.createStatement()) {
                st.execute(query);
                return null;
            }
        });
    }

    public <T> T execute(String query, CheckedFunction<PreparedStatement, T, SQLException> callback) throws SQLException {
        return map(c -> {
            try (PreparedStatement ps = c.prepareStatement(query)) {
                return callback.apply(ps);
            }
        });
    }

    public <T> T execute(String query, CheckedConsumer<PreparedStatement, SQLException> prepare,
            CheckedFunction<ResultSet, T, SQLException> mapper) throws SQLException {
        return execute(query, ps -> {
            prepare.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return mapper.apply(rs);
            }
        });
    }

    public <T> T query(String q, CheckedFunction<ResultSet, T, SQLException> mapper, Object... args) throws SQLException {
        CheckedConsumer<PreparedStatement, SQLException> p = ps -> {
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
            }
        };

        return execute(q, p, mapper);
    }

    public <T> T queryObject(String q, Class<T> type, Object...args) throws Exception {
        return query(q, singleResult(type), args);
    }

    public <T> List<T> queryForList(String q, CheckedBiFunction<ResultSet, Integer, T, SQLException> mapper, Object... args)
            throws Exception {
        CheckedFunction<ResultSet, List<T>, SQLException> f = rs -> {
            List<T> list = new ArrayList<>();
            while (rs.next()) {
                list.add(mapper.apply(rs, rs.getRow()));
            }
            return list;
        };

        return query(q, f, args);
    }

    public <T> List<T> queryForList(String q, Class<T> type, Object... args) throws Exception {
        CheckedBiFunction<ResultSet, Integer, T, SQLException> mapper = (rs, i) -> {
            if (i != 1) {
                throw new IllegalArgumentException("Expected exactly one column...");
            }
            return convertObject(rs.getObject(i), type);
        };
        return queryForList(q, mapper, args);
    }

    public static <T> CheckedFunction<ResultSet, T, SQLException> singleResult(Class<T> type) {
        return rs -> {
            if (rs.next()) {
                T result = convertObject(rs.getObject(1), type);
                if (!rs.next()) {
                    return result;
                }
            }
            throw new IllegalArgumentException("Expected exactly one column; discovered [" + rs.getMetaData().getColumnCount() + "]");
        };
    }


    @SuppressWarnings("unchecked")
    private static <T> T convertObject(Object val, Class<T> type) {
        Object conv = null;

        if (val == null) {
            return null;
        }

        if (String.class == type) {
            conv = val.toString();
        }
        else if (Number.class.isAssignableFrom(type)) {
            Number n = (Number) val;
            if (Integer.class == type) {
                conv = Integer.valueOf(n.intValue());
            }
            else if (Long.class == type) {
                conv = Long.valueOf(n.longValue());
            }
            else {
                throw new IllegalStateException("Unknown type");
            }
        }

        return (T) conv;
    }
    
    public List<Map<String, Object>> queryForList(String q, Object... args) throws Exception {
        return queryForList(q, (rs, i) -> {
            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();
            Map<String, Object> map = new LinkedHashMap<>(count);

            for (int j = 1; j <= count; j++) {
                map.put(metaData.getColumnName(j), rs.getObject(j));
            }
            return map;
        }, args);
    }

    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R, E extends Exception> {
        R apply(T t, U u) throws E;
    }
}