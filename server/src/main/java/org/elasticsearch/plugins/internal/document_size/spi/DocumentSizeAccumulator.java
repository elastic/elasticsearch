/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.document_size.spi;

import java.util.Map;

public interface DocumentSizeAccumulator {

    void add(long size);

    Map<String, String> aggregate(Map<String, String> newUserData, Map<String, String> prevUserDataMap);
}
