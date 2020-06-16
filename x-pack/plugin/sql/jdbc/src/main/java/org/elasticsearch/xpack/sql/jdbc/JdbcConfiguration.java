/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.client.StringUtils;

import java.net.URI;
import java.sql.DriverPropertyInfo;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.sql.client.UriUtils.parseURI;
import static org.elasticsearch.xpack.sql.client.UriUtils.removeQuery;

/**
 / Supports the following syntax
 /
 / jdbc:es://[host|ip]
 / jdbc:es://[host|ip]:port/(prefix)
 / jdbc:es://[host|ip]:port/(prefix)(?options=value&amp;)
 /
 / Additional properties can be specified either through the Properties object or in the URL. In case of duplicates, the URL wins.
 */
//TODO: beef this up for Security/SSL
public class JdbcConfiguration extends ConnectionConfiguration {
    static final String URL_PREFIX = "jdbc:es://";
    public static URI DEFAULT_URI = URI.create("http://localhost:9200/");


    static final String DEBUG = "debug";
    static final String DEBUG_DEFAULT = "false";

    static final String DEBUG_OUTPUT = "debug.output";
    // can be out/err/url
    static final String DEBUG_OUTPUT_DEFAULT = "err";

    static final String DEBUG_FLUSH_ALWAYS = "debug.flushAlways";
    // can be buffered/immediate
    static final String DEBUG_FLUSH_ALWAYS_DEFAULT = "false";

    public static final String TIME_ZONE = "timezone";
    // follow the JDBC spec and use the JVM default...
    // to avoid inconsistency, the default is picked up once at startup and reused across connections
    // to cater to the principle of least surprise
    // really, the way to move forward is to specify a calendar or the timezone manually
    static final String TIME_ZONE_DEFAULT = TimeZone.getDefault().getID();

    static final String FIELD_MULTI_VALUE_LENIENCY = "field.multi.value.leniency";
    static final String FIELD_MULTI_VALUE_LENIENCY_DEFAULT = "true";

    static final String INDEX_INCLUDE_FROZEN = "index.include.frozen";
    static final String INDEX_INCLUDE_FROZEN_DEFAULT = "false";


    // options that don't change at runtime
    private static final Set<String> OPTION_NAMES = new LinkedHashSet<>(
            Arrays.asList(TIME_ZONE, FIELD_MULTI_VALUE_LENIENCY, INDEX_INCLUDE_FROZEN, DEBUG, DEBUG_OUTPUT, DEBUG_FLUSH_ALWAYS));

    static {
        // trigger version initialization
        // typically this should have already happened but in case the
        // EsDriver/EsDataSource are not used and the impl. classes used directly
        // this covers that case
        ClientVersion.CURRENT.toString();
    }

    // immutable properties
    private final boolean debug;
    private final String debugOut;
    private final boolean flushAlways;

    // mutable ones
    private ZoneId zoneId;
    private boolean fieldMultiValueLeniency;
    private boolean includeFrozen;

    public static JdbcConfiguration create(String u, Properties props, int loginTimeoutSeconds) throws JdbcSQLException {
        URI uri = parseUrl(u);
        Properties urlProps = parseProperties(uri, u);
        uri = removeQuery(uri, u, DEFAULT_URI);

        // override properties set in the URL with the ones specified programmatically
        if (props != null) {
            urlProps.putAll(props);
        }

        if (loginTimeoutSeconds > 0) {
            urlProps.setProperty(CONNECT_TIMEOUT, Long.toString(TimeUnit.SECONDS.toMillis(loginTimeoutSeconds)));
        }

        try {
            return new JdbcConfiguration(uri, u, urlProps);
        } catch (JdbcSQLException e) {
            throw e;
        } catch (Exception ex) {
            throw new JdbcSQLException(ex, ex.getMessage());
        }
    }

    private static URI parseUrl(String u) throws JdbcSQLException {
        String url = u;
        String format = "jdbc:es://[[http|https]://]?[host[:port]]?/[prefix]?[\\?[option=value]&]*";
        if (!canAccept(u)) {
            throw new JdbcSQLException("Expected [" + URL_PREFIX + "] url, received [" + u + "]");
        }

        try {
            return parseURI(removeJdbcPrefix(u), DEFAULT_URI);
        } catch (IllegalArgumentException ex) {
            throw new JdbcSQLException(ex, "Invalid URL [" + url + "], format should be [" + format + "]");
        }
    }

    private static String removeJdbcPrefix(String connectionString) throws JdbcSQLException {
        if (connectionString.startsWith(URL_PREFIX)) {
            return connectionString.substring(URL_PREFIX.length());
        } else {
            throw new JdbcSQLException("Expected [" + URL_PREFIX + "] url, received [" + connectionString + "]");
        }
    }

    private static Properties parseProperties(URI uri, String u) throws JdbcSQLException {
        Properties props = new Properties();
        try {
            if (uri.getRawQuery() != null) {
                // parse properties
                List<String> prms = StringUtils.tokenize(uri.getRawQuery(), "&");
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
        return props;
    }

    // constructor is private to force the use of a factory in order to catch and convert any validation exception
    // and also do input processing as oppose to handling this from the constructor (which is tricky or impossible)
    private JdbcConfiguration(URI baseURI, String u, Properties props) throws JdbcSQLException {
        super(baseURI, u, props);

        this.debug = parseValue(DEBUG, props.getProperty(DEBUG, DEBUG_DEFAULT), Boolean::parseBoolean);
        this.debugOut = props.getProperty(DEBUG_OUTPUT, DEBUG_OUTPUT_DEFAULT);
        this.flushAlways = parseValue(DEBUG_FLUSH_ALWAYS, props.getProperty(DEBUG_FLUSH_ALWAYS, DEBUG_FLUSH_ALWAYS_DEFAULT),
                Boolean::parseBoolean);

        this.zoneId = parseValue(TIME_ZONE, props.getProperty(TIME_ZONE, TIME_ZONE_DEFAULT),
                s -> TimeZone.getTimeZone(s).toZoneId().normalized());
        this.fieldMultiValueLeniency = parseValue(FIELD_MULTI_VALUE_LENIENCY,
                props.getProperty(FIELD_MULTI_VALUE_LENIENCY, FIELD_MULTI_VALUE_LENIENCY_DEFAULT), Boolean::parseBoolean);
        this.includeFrozen = parseValue(INDEX_INCLUDE_FROZEN, props.getProperty(INDEX_INCLUDE_FROZEN, INDEX_INCLUDE_FROZEN_DEFAULT),
                Boolean::parseBoolean);
    }

    @Override
    protected Collection<String> extraOptions() {
        return OPTION_NAMES;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    public boolean debug() {
        return debug;
    }

    public String debugOut() {
        return debugOut;
    }

    public boolean flushAlways() {
        return flushAlways;
    }

    public TimeZone timeZone() {
        return zoneId != null ? TimeZone.getTimeZone(zoneId) : null;
    }

    public boolean fieldMultiValueLeniency() {
        return fieldMultiValueLeniency;
    }

    public boolean indexIncludeFrozen() {
        return includeFrozen;
    }

    public static boolean canAccept(String url) {
        return (StringUtils.hasText(url) && url.trim().startsWith(JdbcConfiguration.URL_PREFIX));
    }

    public DriverPropertyInfo[] driverPropertyInfo() {
        List<DriverPropertyInfo> info = new ArrayList<>();
        for (String option : optionNames()) {
            DriverPropertyInfo prop = new DriverPropertyInfo(option, null);
            info.add(prop);
        }

        return info.toArray(new DriverPropertyInfo[info.size()]);
    }
}
