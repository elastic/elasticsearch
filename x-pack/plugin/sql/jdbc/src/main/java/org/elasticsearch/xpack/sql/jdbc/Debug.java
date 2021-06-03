/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.client.SuppressForbidden;

import javax.sql.DataSource;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Class handling debug logging. Typically disabled (hence why it's called debug).
 * JDBC carries a lot of legacy conventions, logging being one of them - in JDBC logging was expected to
 * be to System.Err/Out since there were no logging frameworks at the time.
 * This didn't work so the API was changed through {@link DriverManager#getLogStream()} however that also had issues
 * being global and not working well with encoding (hence why {@link DriverManager#getLogWriter()} was introduced)
 * and was changed again through {@link DataSource#getLogWriter()}.
 * However by then the damage was done and most drivers don't use either and have their own logging implementation.
 *
 * This class tries to cater to both audiences - use the legacy, Writer way if needed though strive to use the
 * proper typical approach, that of specifying intention and output (file) in the URL.
 *
 * For this reason the {@link System#out} and {@link System#err} are being referred in this class though are used only
 * when needed.
 */
final class Debug {

    // cache for streams created by ourselves
    private static final Map<String, DebugLog> OUTPUT_CACHE = new HashMap<>();
    // reference counter for a given output
    private static final Map<String, Integer> OUTPUT_REFS = new HashMap<>();
    // cache of loggers that rely on external/managed printers
    private static final Map<PrintWriter, DebugLog> OUTPUT_MANAGED = new HashMap<>();

    private static volatile DebugLog ERR = null, OUT = null;
    private static volatile PrintStream SYS_ERR = null, SYS_OUT = null;

    /**
     * Create a proxied Connection which performs logging of all methods being invoked.
     * Typically Debug will read its configuration from the configuration and act accordingly however
     * there are two cases where the output is specified programmatically, namely through
     * {@link DriverManager#setLogWriter(PrintWriter)} and {@link DataSource#setLogWriter(PrintWriter)}.
     * The former is the 'legacy' way, having a global impact on all drivers while the latter allows per
     * instance configuration.
     *
     * As both approaches are not widely used, Debug will take the principle of least surprise and pick its
     * own configuration first; if that does not exist it will fallback to the managed approaches (assuming they
     * are specified, otherwise logging is simply disabled).
     */
    static Connection proxy(JdbcConfiguration info, Connection connection, PrintWriter managedPrinter) {
        return createProxy(Connection.class, new ConnectionProxy(logger(info, managedPrinter), connection));
    }

    static DatabaseMetaData proxy(DatabaseMetaDataProxy handler) {
        return createProxy(DatabaseMetaData.class, handler);
    }

    static ParameterMetaData proxy(ParameterMetaDataProxy handler) {
        return createProxy(ParameterMetaData.class, handler);
    }

    static ResultSet proxy(ResultSetProxy handler) {
        return createProxy(ResultSet.class, handler);
    }

    static ResultSetMetaData proxy(ResultSetMetaDataProxy handler) {
        return createProxy(ResultSetMetaData.class, handler);
    }

    static Statement proxy(Object statement, StatementProxy handler) {
        Class<? extends Statement> i = Statement.class;

        if (statement instanceof CallableStatement) {
            i = CallableStatement.class;
        }
        else if (statement instanceof PreparedStatement) {
            i = PreparedStatement.class;
        }

        return createProxy(i, handler);
    }

    @SuppressWarnings("unchecked")
    private static <P> P createProxy(Class<P> proxy, InvocationHandler handler) {
        return (P) Proxy.newProxyInstance(Debug.class.getClassLoader(), new Class<?>[] { DebugProxy.class, proxy }, handler);
    }

    private static DebugLog logger(JdbcConfiguration info, PrintWriter managedPrinter) {
        DebugLog log = null;

        if (managedPrinter != null) {
            synchronized (Debug.class) {
                log = OUTPUT_MANAGED.get(managedPrinter);
                if (log == null) {
                    log = createLog(managedPrinter, info.flushAlways());
                    OUTPUT_MANAGED.put(managedPrinter, log);
                }
            }
        }

        String out = info.debugOut();

        // System.out/err can be changed so do some checks
        if ("err".equals(out)) {
            PrintStream sys = stderr();

            if (SYS_ERR == null) {
                SYS_ERR = sys;
            }
            if (SYS_ERR != sys) {
                SYS_ERR.flush();
                SYS_ERR = sys;
                ERR = null;
            }
            if (ERR == null) {
                ERR = createLog(new PrintWriter(new OutputStreamWriter(sys, StandardCharsets.UTF_8)), info.flushAlways());
            }
            return ERR;
        }

        if ("out".equals(out)) {
            PrintStream sys = stdout();

            if (SYS_OUT == null) {
                SYS_OUT = sys;
            }

            if (SYS_OUT != sys) {
                SYS_OUT.flush();
                SYS_OUT = sys;
                OUT = null;
            }

            if (OUT == null) {
                OUT = createLog(new PrintWriter(new OutputStreamWriter(sys, StandardCharsets.UTF_8)), info.flushAlways());
            }
            return OUT;
        }

        synchronized (Debug.class) {
            log = OUTPUT_CACHE.get(out);
            if (log == null) {
                // must be local file
                try {
                    PrintWriter print = new PrintWriter(Files.newBufferedWriter(Paths.get("").resolve(out), StandardCharsets.UTF_8));
                    log = createLog(print, info.flushAlways());
                    OUTPUT_CACHE.put(out, log);
                    OUTPUT_REFS.put(out, Integer.valueOf(0));
                } catch (Exception ex) {
                    throw new JdbcException(ex, "Cannot open debug output [" + out + "]");
                }
            }
            OUTPUT_REFS.put(out, Integer.valueOf(OUTPUT_REFS.get(out).intValue() + 1));
        }

        return log;
    }

    private static DebugLog createLog(PrintWriter print, boolean flushAlways) {
        DebugLog log = new DebugLog(print, flushAlways);
        log.logSystemInfo();
        return log;
    }

    static void release(JdbcConfiguration info) {
        if (info.debug() == false) {
            return;
        }

        String out = info.debugOut();
        synchronized (Debug.class) {
            Integer ref = OUTPUT_REFS.get(out);
            if (ref != null) {
                int r = ref.intValue();
                if (r < 2) {
                    OUTPUT_REFS.remove(out);
                    DebugLog d = OUTPUT_CACHE.remove(out);
                    if (d != null) {
                        if (d.print != null) {
                            d.print.close();
                        }
                    }
                }
                else {
                    OUTPUT_REFS.put(out, Integer.valueOf(r - 1));
                }
            }
        }
    }

    static synchronized void close() {
        // clear the ref
        OUTPUT_REFS.clear();

        // clear the streams
        for (DebugLog d : OUTPUT_CACHE.values()) {
            if (d.print != null) {
                d.print.close();
            }
        }
        OUTPUT_CACHE.clear();

        // flush the managed ones
        for (DebugLog d : OUTPUT_MANAGED.values()) {
            d.print.flush();
        }

        OUTPUT_MANAGED.clear();
    }

    @SuppressForbidden(reason = "JDBC drivers allows logging to Sys.out")
    private static PrintStream stdout() {
        return System.out;
    }

    @SuppressForbidden(reason = "JDBC drivers allows logging to Sys.err")
    private static PrintStream stderr() {
        return System.err;
    }
}
