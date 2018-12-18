/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class RequestInfo {
    public static final String CLI = "cli";
    private static final String CANVAS = "canvas";
    public static final String ODBC32 = "odbc32";
    private static final String ODBC64 = "odbc64";
    public static final List<String> CLIENT_IDS = Arrays.asList(CLI, CANVAS, ODBC32, ODBC64);
    public static final List<String> ODBC_CLIENT_IDS =  Arrays.asList(ODBC32, ODBC64);
    
    private Mode mode;
    private String clientId;
    
    public RequestInfo(Mode mode) {
        this(mode, null);
    }
    
    public RequestInfo(Mode mode, String clientId) {
        mode(mode);
        clientId(clientId);
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
