/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.util.Locale;
import java.util.Objects;

public class RequestInfo {
    public static final String CLI = "cli";
    public static final String CANVAS = "canvas";
    
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
            if (!clientId.equals(CLI) && !clientId.equals(CANVAS)) {
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
