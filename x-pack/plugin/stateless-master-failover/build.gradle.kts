/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */
esplugin {
    name = "serverless-master-failover"
    description = "Triggers graceful master failover on shutdown for serverless Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.shutdown.masterfailover.MasterFailoverPlugin"
    extendedPlugins = listOf("x-pack-core")
}

dependencies {
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("security"))
}
