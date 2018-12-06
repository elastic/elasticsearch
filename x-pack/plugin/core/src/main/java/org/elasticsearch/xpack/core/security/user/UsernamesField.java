/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.user;

public final class UsernamesField {
    public static final String ELASTIC_NAME = "elastic";
    public static final String ELASTIC_ROLE = "superuser";
    public static final String KIBANA_NAME = "kibana";
    public static final String KIBANA_ROLE = "kibana_system";
    public static final String SYSTEM_NAME = "_system";
    public static final String SYSTEM_ROLE = "_system";
    public static final String XPACK_SECURITY_NAME = "_xpack_security";
    public static final String XPACK_SECURITY_ROLE = "superuser";
    public static final String XPACK_NAME = "_xpack";
    public static final String XPACK_ROLE =  "_xpack";
    public static final String LOGSTASH_NAME = "logstash_system";
    public static final String LOGSTASH_ROLE = "logstash_system";
    public static final String BEATS_NAME = "beats_system";
    public static final String BEATS_ROLE = "beats_system";
    public static final String APM_NAME = "apm_system";
    public static final String APM_ROLE = "apm_system";

    public static final String REMOTE_MONITORING_NAME = "remote_monitoring_user";
    public static final String REMOTE_MONITORING_COLLECTION_ROLE = "remote_monitoring_collector";
    public static final String REMOTE_MONITORING_INDEXING_ROLE = "remote_monitoring_agent";

    private UsernamesField() {}
}
