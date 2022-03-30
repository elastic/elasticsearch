/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import java.util.Map;

public interface ESMapMessage extends Message {

    ESMapMessage argAndField(String key, Object value);

    ESMapMessage field(String key, Object value);

    ESMapMessage withFields(Map<String, Object> prepareMap);

    Object[] getArguments();

    String getMessagePattern();

    String get(String key);
}
