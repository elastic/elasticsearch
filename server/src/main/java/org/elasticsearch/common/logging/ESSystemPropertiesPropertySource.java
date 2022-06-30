/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.util.SystemPropertiesPropertySource;

// This sucks!, but is resolved in log4J 2.18, as part of https://issues.apache.org/jira/browse/LOG4J2-3427
public class ESSystemPropertiesPropertySource extends SystemPropertiesPropertySource {

    public ESSystemPropertiesPropertySource() {}
}
