/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.jose {
    requires org.elasticsearch.server;
    requires com.nimbusds.jose.jwt;
    requires oauth2.oidc.sdk;

    exports org.elasticsearch.nimbus;
}
