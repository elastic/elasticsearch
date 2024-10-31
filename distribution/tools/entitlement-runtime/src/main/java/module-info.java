/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.entitlement.runtime {
    // Note that the bridge will be in java.base here
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.logging;

    exports org.elasticsearch.entitlement.runtime.api;

//    provides org.elasticsearch.entitlement.api.EntitlementChecks
//        with
//            org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementManager;
}
