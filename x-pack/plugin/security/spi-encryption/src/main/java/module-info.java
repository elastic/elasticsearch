/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.security.spi.encryption {
    requires org.elasticsearch.server;

    exports org.elasticsearch.xpack.security.spi.encryption;
}
