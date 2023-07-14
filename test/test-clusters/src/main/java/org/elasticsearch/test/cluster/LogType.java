/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

public enum LogType {
    SERVER("elasticsearch.log"),
    SERVER_JSON("elasticsearch_server.json");

    private final String filename;

    LogType(String filenameFormat) {
        this.filename = filenameFormat;
    }

    public String getFilename() {
        return filename;
    }
}
