/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public class RequestInfo {
    private static final String CANVAS = "canvas";
    public static final String ODBC_32 = "odbc32";
    private static final String ODBC_64 = "odbc64";
    public static final Set<String> CLIENT_IDS;
    public static final Set<String> ODBC_CLIENT_IDS;
    
    static {
        Set<String> clientIds = new HashSet<>(4);
        clientIds.add(CANVAS);
        clientIds.add(ODBC_32);
        clientIds.add(ODBC_64);

        Set<String> odbcClientIds = new HashSet<>(2);
        odbcClientIds.add(ODBC_32);
        odbcClientIds.add(ODBC_64);
        
        CLIENT_IDS = Collections.unmodifiableSet(clientIds);
        ODBC_CLIENT_IDS = Collections.unmodifiableSet(odbcClientIds);
    }
    
    private Mode mode;
    private String clientId;
    private SqlVersion version;

    public RequestInfo(Mode mode) {
        this(mode, null, null);
    }
    
    public RequestInfo(Mode mode, String clientId) {
        this(mode, clientId, null);
    }

    public RequestInfo(Mode mode, String clientId, String version) {
        mode(mode);
        clientId(clientId);
        version(version);
    }

    public RequestInfo(Mode mode, SqlVersion version) {
        mode(mode);
        this.version = version;
    }
    
    public Mode mode() {
        return mode;
    }
    
    public void mode(Mode mode) {
        this.mode = mode;
    }
    
    public String clientId() {
        return clientId;
    }
    
    public void clientId(String clientId) {
        if (clientId != null) {
            clientId = clientId.toLowerCase(Locale.ROOT);
            if (false == CLIENT_IDS.contains(clientId)) {
                clientId = null;
            }
        }
        this.clientId = clientId;
    }

    public void version(String clientVersion) {
        this.version = SqlVersion.fromString(clientVersion);
    }

    public SqlVersion version() {
        return version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, clientId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestInfo that = (RequestInfo) o;
        return Objects.equals(mode, that.mode) && Objects.equals(clientId, that.clientId);
    }
}
