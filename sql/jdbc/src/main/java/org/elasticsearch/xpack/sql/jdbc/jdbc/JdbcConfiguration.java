/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.xpack.sql.jdbc.util.Assert;
import org.elasticsearch.xpack.sql.net.client.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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

    static final String USER = "user";

    static final String PASSWORD = "password";

    static final String DEBUG = "debug";
    static final String DEBUG_DEFAULT = "false";

    static final String DEBUG_OUTPUT = "debug.output";
    // can be out/err/url
    static final String DEBUG_OUTPUT_DEFAULT = "err";

    static final String TIME_ZONE = "time_zone";
    static final String TIME_ZONE_DEFAULT = "UTC";

    private static final List<String> KNOWN_OPTIONS = Arrays.asList(DEBUG, DEBUG_OUTPUT, TIME_ZONE);

    private HostAndPort hostAndPort;
    private String originalUrl;
    private String urlFile = "/";

    private boolean debug = false;
    private String debugOut = DEBUG_OUTPUT_DEFAULT;
    private final TimeZone timeZone;

    public JdbcConfiguration(String u, Properties props) {
        super(props);
        originalUrl = u;
        parseUrl(u);

        Properties set = settings();
        debug = Boolean.parseBoolean(set.getProperty(DEBUG, DEBUG_DEFAULT));
        debugOut = settings().getProperty(DEBUG_OUTPUT, DEBUG_OUTPUT_DEFAULT);
        timeZone = TimeZone.getTimeZone(settings().getProperty(TIME_ZONE, TIME_ZONE_DEFAULT));
    }

    private void parseUrl(String u) {
        String url = u;
        String format = "jdbc:es://[host[:port]]*/[prefix]*[?[option=value]&]*";
        if (!canAccept(u)) {
            throw new JdbcException("Expected %s url, received %s", URL_PREFIX, u);
        }

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
                throw new JdbcException("Invalid URL %s, format should be %s", url, format);
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
                    throw new JdbcException("Invalid URL %s, format should be %s", url, format);
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
                    throw new JdbcException("Invalid port specified");
                }
                String host = hostAndPort.substring(0, index);
                String port = hostAndPort.substring(index + 1);

                this.hostAndPort = new HostAndPort(host, Integer.parseInt(port));
            } else {
                this.hostAndPort = new HostAndPort(hostAndPort);
            }

            //
            // parse params
            //
            if (params != null) {
                // parse properties
                List<String> prms = StringUtils.tokenize(params, "&");
                for (String param : prms) {
                    List<String> args = StringUtils.tokenize(param, "=");
                    Assert.isTrue(args.size() == 2, "Invalid parameter %s, format needs to be key=value", param);
                    String pName = args.get(0);
                    if (!KNOWN_OPTIONS.contains(pName)) {
                        throw new JdbcException("Unknown parameter [%s] ; did you mean %s", pName,
                                StringUtils.findSimiliar(pName, KNOWN_OPTIONS));
                    }
                    
                    settings().setProperty(args.get(0), args.get(1));
                }
            }
        } catch (JdbcException e) {
            throw e;
        } catch (Exception e) {
            // Add the url to unexpected exceptions
            throw new IllegalArgumentException("Failed to parse acceptable jdbc url [" + u + "]", e);
        }
    }

    public URL asUrl() {
        // TODO: need to assemble all the various params here
        try {
            return new URL(isSSL() ? "https" : "http", hostAndPort.ip, port(), urlFile);
        } catch (MalformedURLException ex) {
            throw new JdbcException(ex, "Cannot connect to server %s", originalUrl);
        }
    }

    public String userName() {
        return settings().getProperty(USER);
    }

    public String password() {
        // NOCOMMIT make sure we're doing right by the password. Compare with other jdbc drivers and our security code.
        return settings().getProperty(PASSWORD);
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

    public static boolean canAccept(String url) {
        return (StringUtils.hasText(url) && url.trim().startsWith(JdbcConfiguration.URL_PREFIX));
    }

    public DriverPropertyInfo[] driverPropertyInfo() {
        List<DriverPropertyInfo> info = new ArrayList<>();
        for (String option : KNOWN_OPTIONS) {
            String value = null;
            DriverPropertyInfo prop = new DriverPropertyInfo(option, value);
            info.add(prop);
        }
        
        return info.toArray(new DriverPropertyInfo[info.size()]);
    }
}