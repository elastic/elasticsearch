/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.ParseField;

final class SqlField {
    public static final ParseField MODE = new ParseField("mode");
    public static final ParseField CLIENT_ID = new ParseField("client.id");
    
    private SqlField() {}
}
