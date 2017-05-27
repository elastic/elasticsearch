/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util;

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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;

// poor's man JdbcTemplate
public class JdbcTemplate {

    private final Supplier<Connection> conn;

    public JdbcTemplate(JdbcSupplier<Connection> conn) {
        this.conn = conn;
    }

    public static interface JdbcSupplier<T> extends Supplier<T> {

        @Override
        default T get() {
            try {
                return jdbc();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        T jdbc() throws SQLException;
    }

    public static interface JdbcConsumer<T> extends Consumer<T> {

        @Override
        default void accept(T t) {
            try {
                jdbc(t);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        void jdbc(T t) throws SQLException;
    }

    public static interface JdbcFunction<T, R> extends Function<T, R> {

        @Override
        default R apply(T t) {
            try {
                return jdbc(t);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        R jdbc(T t) throws SQLException;
    }

    public static interface JdbcBiFunction<T, U, R> extends BiFunction<T, U, R> {

        @Override
        default R apply(T t, U u) {
            try {
                return jdbc(t, u);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        R jdbc(T t, U u) throws SQLException;
    }


    private static final int MAX_WIDTH = 20;

    public static JdbcFunction<ResultSet, Void> resultSetToConsole() {
        return rs -> {
            ResultSetMetaData metaData = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            StringBuilder column = new StringBuilder();

            int columns = metaData.getColumnCount();
            for (int i = 1; i <= columns; i++) {
                if (i > 1) {
                    sb.append(" | ");
                }
                column.setLength(0);
                column.append(metaData.getColumnName(i));
                column.append("(");
                column.append(metaData.getColumnTypeName(i));
                column.append(")");

                sb.append(trimOrPad(column));
            }

            int l = sb.length();
            sb.append("\n");
            for (int i = 0; i < l; i++) {
                sb.append("=");
            }
            System.out.println(sb);

            while (rs.next()) {
                sb.setLength(0);
                for (int i = 1; i <= columns; i++) {
                    column.setLength(0);
                    if (i > 1) {
                        sb.append(" | ");
                    }
                    sb.append(trimOrPad(column.append(rs.getString(i))));
                }
                System.out.println(sb);
            }
            return null;
        };
    }

    private static StringBuilder trimOrPad(StringBuilder buffer) {
        if (buffer.length() > MAX_WIDTH) {
            buffer.setLength(MAX_WIDTH - 1);
            buffer.append("~");
        }
        else {
            for (int i = buffer.length(); i < MAX_WIDTH; i++) {
                buffer.append(" ");
            }
        }
        return buffer;
    }

    public void con(JdbcConsumer<Connection> c) throws Exception {
        try (Connection con = conn.get()) {
            c.accept(con);
        }
    }

    public <T> T con(JdbcFunction<Connection, T> c) throws Exception {
        try (Connection con = conn.get()) {
            return c.apply(con);
        }
    }

    public <T> T query(String q, JdbcFunction<ResultSet, T> f) throws Exception {
        return con(c -> {
            try (Statement st = c.createStatement();
                 ResultSet rset = st.executeQuery(q)) {
                return f.apply(rset);
            }
        });
    }

    public void queryToConsole(String q) throws Exception {
        query(q, resultSetToConsole());
    }

    public <T> T queryObject(String q, Class<T> type) throws Exception {
        return query(q, singleResult(type));
    }

    public void execute(String query) throws Exception {
        con(c -> {
            try (Statement st = c.createStatement()) {
                st.execute(query);
                return null;
            }
        });
    }

    public <T> T execute(String query, JdbcFunction<PreparedStatement, T> callback) throws Exception {
        return con(c -> {
            try (PreparedStatement ps = c.prepareStatement(query)) {
                return callback.apply(ps);
            }
        });
    }

    public <T> T execute(String query, JdbcConsumer<PreparedStatement> prepare, JdbcFunction<ResultSet, T> mapper) throws Exception {
        return execute(query, ps -> {
            prepare.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return mapper.apply(rs);
            }
        });
    }

    public <T> T query(String q, JdbcFunction<ResultSet, T> mapper, Object... args) throws Exception {
        JdbcConsumer<PreparedStatement> p = ps -> {
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

    public <T> List<T> queryForList(String q, JdbcBiFunction<ResultSet, Integer, T> mapper, Object... args) throws Exception {
        JdbcFunction<ResultSet, List<T>> f = rs -> {
            List<T> list = new ArrayList<>();
            while (rs.next()) {
                list.add(mapper.apply(rs, rs.getRow()));
            }
            return list;
        };

        return query(q, f, args);
    }

    public <T> List<T> queryForList(String q, Class<T> type, Object... args) throws Exception {
        JdbcBiFunction<ResultSet, Integer, T> mapper = (rs, i) -> {
            if (i != 1) {
                throw new IllegalArgumentException("Expected exactly one column...");
            }
            return convertObject(rs.getObject(i), type);
        };
        return queryForList(q, mapper, args);
    }

    public static <T> JdbcFunction<ResultSet, T> singleResult(Class<T> type) {
        return rs -> {
            if (rs.next()) {
                T result = convertObject(rs.getObject(1), type);
                if (!rs.next()) {
                    return result;
                }
            }
            throw new IllegalArgumentException(format("Expected exactly one column; discovered %s",rs.getMetaData().getColumnCount()));
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

            for (int j = 0; j < count; j++) {
                map.put(metaData.getColumnName(j), rs.getObject(j));
            }
            return map;
        }, args);
    }
}