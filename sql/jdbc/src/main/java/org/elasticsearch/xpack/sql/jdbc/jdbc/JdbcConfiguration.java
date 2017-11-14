/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.xpack.sql.client.shared.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.client.shared.StringUtils;
import org.elasticsearch.xpack.sql.jdbc.JdbcSQLException;
import org.elasticsearch.xpack.sql.jdbc.util.Version;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

//
// Supports the following syntax
//
// jdbc:es:
// jdbc:es://[host|ip]
// jdbc:es://[host|ip]:port/(prefix)
// jdbc:es://[host|ip]:port/(prefix)(?options=value&)
// 
// Additional properties can be specified either through the Properties object or in the URL. In case of duplicates, the URL wins.
//

//TODO: beef this up for Security/SSL
public class JdbcConfiguration extends ConnectionConfiguration {
    static final String URL_PREFIX = "jdbc:es:";

    static final String DEBUG = "debug";
    static final String DEBUG_DEFAULT = "false";

    static final String DEBUG_OUTPUT = "debug.output";
    // can be out/err/url
    static final String DEBUG_OUTPUT_DEFAULT = "err";

    public static final String TIME_ZONE = "timezone";
    // follow the JDBC spec and use the JVM default...
    // to avoid inconsistency, the default is picked up once at startup and reused across connections
    // to cater to the principle of least surprise 
    // really, the way to move forward is to specify a calendar or the timezone manually
    static final String TIME_ZONE_DEFAULT = TimeZone.getDefault().getID();

    // options that don't change at runtime
    private static final Set<String> OPTION_NAMES = new LinkedHashSet<>(Arrays.asList(TIME_ZONE, DEBUG, DEBUG_OUTPUT));

    static {
        // trigger version initialization
        // typically this should have already happened but in case the
        // JdbcDriver/JdbcDataSource are not used and the impl. classes used directly
        // this covers that case
        Version.version();
    }

    // immutable properties
    private final HostAndPort hostAndPort;
    private final String originalUrl;
    private final String urlFile;
    private final boolean debug;
    private final String debugOut;

    // mutable ones
    private TimeZone timeZone;

    public static JdbcConfiguration create(String u, Properties props) throws JdbcSQLException {
        Object[] result = parseUrl(u);

        String urlFile = (String) result[0];
        HostAndPort hostAndPort = (HostAndPort) result[1];
        Properties urlProps = (Properties) result[2];

        // override properties set in the URL with the ones specified programmatically
        if (props != null) {
            urlProps.putAll(props);
        }

        try {
            return new JdbcConfiguration(u, urlFile, hostAndPort, urlProps);
        } catch (JdbcSQLException e) {
            throw e;
        } catch (Exception ex) {
            throw new JdbcSQLException(ex, ex.getMessage());
        }
    }

    private static Object[] parseUrl(String u) throws JdbcSQLException {
        String url = u;
        String format = "jdbc:es://[host[:port]]*/[prefix]*[?[option=value]&]*";
        if (!canAccept(u)) {
            throw new JdbcSQLException("Expected [" + URL_PREFIX + "] url, received [" + u +"]");
        }

        String urlFile = "/";
        HostAndPort destination;
        Properties props = new Properties();

        try {
            if (u.endsWith("/")) {
                u = u.substring(0, u.length() - 1);
            }

            // remove space
            u = u.trim();

            //
            // remove prefix jdbc:es prefix
            //
            u = u.substring(URL_PREFIX.length(), u.length());

            if (!u.startsWith("//")) {
                throw new JdbcSQLException("Invalid URL [" + url + "], format should be [" + format + "]");
            }

            // remove //
            u = u.substring(2);

            String hostAndPort = u;

            // / is required if any params are specified
            // get it out of the way early on
            int index = u.indexOf("/");
    
            String params = null;
            int pIndex = u.indexOf("?");
            if (pIndex > 0) {
                if (index < 0) {
                    throw new JdbcSQLException("Invalid URL [" + url + "], format should be [" + format + "]");
                }
                if (pIndex + 1 < u.length()) {
                    params = u.substring(pIndex + 1);
                }
            }

            // parse url suffix (if any)
            if (index >= 0) {
                hostAndPort = u.substring(0, index);
                if (index + 1 < u.length()) {
                    urlFile = u.substring(index);
                    index = urlFile.indexOf("?");
                    if (index > 0) {
                        urlFile = urlFile.substring(0, index);
                        if (!urlFile.endsWith("/")) {
                            urlFile = urlFile + "/";
                        }
                    }
                }
            }

            //
            // parse host
            //

            // look for port
            index = hostAndPort.lastIndexOf(":");
            if (index > 0) {
                if (index + 1 >= hostAndPort.length()) {
                    throw new JdbcSQLException("Invalid port specified");
                }
                String host = hostAndPort.substring(0, index);
                String port = hostAndPort.substring(index + 1);

                destination = new HostAndPort(host, Integer.parseInt(port));
            } else {
                destination = new HostAndPort(hostAndPort);
            }

            //
            // parse params
            //
            
            if (params != null) {
                // parse properties
                List<String> prms = StringUtils.tokenize(params, "&");
                for (String param : prms) {
                    List<String> args = StringUtils.tokenize(param, "=");
                    if (args.size() != 2) {
                        throw new JdbcSQLException("Invalid parameter [" + param + "], format needs to be key=value");
                    }
                    // further validation happens in the constructor (since extra properties might be specified either way)
                    props.setProperty(args.get(0).trim(), args.get(1).trim());
                }
            }
        } catch (JdbcSQLException e) {
            throw e;
        } catch (Exception e) {
            // Add the url to unexpected exceptions
            throw new IllegalArgumentException("Failed to parse acceptable jdbc url [" + u + "]", e);
        }
        
        return new Object[] { urlFile, destination, props };
    }

    // constructor is private to force the use of a factory in order to catch and convert any validation exception
    // and also do input processing as oppose to handling this from the constructor (which is tricky or impossible)
    private JdbcConfiguration(String u, String urlFile, HostAndPort hostAndPort, Properties props) throws JdbcSQLException {
        super(props);

        this.originalUrl = u;
        this.urlFile = urlFile;
        this.hostAndPort = hostAndPort;

        this.debug = parseValue(DEBUG, props.getProperty(DEBUG, DEBUG_DEFAULT), Boolean::parseBoolean);
        this.debugOut = props.getProperty(DEBUG_OUTPUT, DEBUG_OUTPUT_DEFAULT);

        this.timeZone = parseValue(TIME_ZONE, props.getProperty(TIME_ZONE, TIME_ZONE_DEFAULT), TimeZone::getTimeZone);
    }

    @Override
    protected Collection<? extends String> extraOptions() {
        return OPTION_NAMES;
    }

    public URL asUrl() throws JdbcSQLException {
        try {
            return new URL(isSSLEnabled() ? "https" : "http", hostAndPort.ip, port(), urlFile);
        } catch (MalformedURLException ex) {
            throw new JdbcSQLException(ex, "Cannot connect to server [" + originalUrl + "]");
        }
    }

    private int port() {
        return hostAndPort.port > 0 ? hostAndPort.port : 9200;
    }

    public boolean debug() {
        return debug;
    }

    public String debugOut() {
        return debugOut;
    }

    public TimeZone timeZone() {
        return timeZone;
    }

    public void timeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public static boolean canAccept(String url) {
        return (StringUtils.hasText(url) && url.trim().startsWith(JdbcConfiguration.URL_PREFIX));
    }

    public DriverPropertyInfo[] driverPropertyInfo() {
        List<DriverPropertyInfo> info = new ArrayList<>();
        for (String option : OPTION_NAMES) {
            String value = null;
            DriverPropertyInfo prop = new DriverPropertyInfo(option, value);
            info.add(prop);
        }
        
        return info.toArray(new DriverPropertyInfo[info.size()]);
    }
}