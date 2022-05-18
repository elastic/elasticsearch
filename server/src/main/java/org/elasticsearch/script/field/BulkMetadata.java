/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.List;
import java.util.Map;

public interface BulkMetadata {
    List<String> extraKeys();

    String getIndex();

    boolean indexChanged();

    String getId();

    boolean idChanged();

    Long getVersion();

    boolean versionChanged();

    String getRouting();

    boolean routingChanged();

    Op getOp();

    boolean opChanged();

    Map<String, Object> getSource();

    Map<String, Object> getCtx();
}
