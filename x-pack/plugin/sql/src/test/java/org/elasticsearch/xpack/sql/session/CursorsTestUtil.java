/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.Version;

import java.time.ZoneId;

public class CursorsTestUtil {

    public static String encodeToString(Cursor info, Version version, ZoneId zoneId) {
        return Cursors.encodeToString(info, version, zoneId);
    }
}
