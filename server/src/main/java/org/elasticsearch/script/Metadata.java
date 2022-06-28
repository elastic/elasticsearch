/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Metadata availabe to write scripts
 */
public interface Metadata extends Map<String, Object> {
    String getIndex();

    void setIndex(String index);

    String getId();

    void setId(String id);

    String getRouting();

    void setRouting(String routing);

    long getVersion();

    void setVersion(long version);

    String getVersionType();

    void setVersionType(String versionType);

    ZonedDateTime getTimestamp();
}
