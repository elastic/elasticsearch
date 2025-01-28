/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

public final class UsernamesField {
    public static final String ELASTIC_NAME = "elastic";
    public static final String ELASTIC_ROLE = "superuser";
    public static final String DEPRECATED_KIBANA_NAME = "kibana";
    public static final String KIBANA_NAME = "kibana_system";
    public static final String KIBANA_ROLE = "kibana_system";
    public static final String SYSTEM_NAME = "_system";
    public static final String SYSTEM_ROLE = "_system";
    public static final String XPACK_SECURITY_NAME = "_xpack_security";
    public static final String XPACK_SECURITY_ROLE = "_xpack_security";
    public static final String DATA_STREAM_LIFECYCLE_NAME = "_data_stream_lifecycle";
    public static final String DATA_STREAM_LIFECYCLE_ROLE = "_data_stream_lifecycle";
    public static final String SECURITY_PROFILE_NAME = "_security_profile";
    public static final String SECURITY_PROFILE_ROLE = "_security_profile";
    public static final String XPACK_NAME = "_xpack";
    public static final String XPACK_ROLE = "_xpack";
    public static final String LOGSTASH_NAME = "logstash_system";
    public static final String LOGSTASH_ROLE = "logstash_system";
    public static final String BEATS_NAME = "beats_system";
    public static final String BEATS_ROLE = "beats_system";
    public static final String APM_NAME = "apm_system";
    public static final String APM_ROLE = "apm_system";
    public static final String ASYNC_SEARCH_NAME = "_async_search";
    public static final String ASYNC_SEARCH_ROLE = "_async_search";
    public static final String STORAGE_USER_NAME = "_storage";
    public static final String STORAGE_ROLE_NAME = "_storage";
    public static final String SYNONYMS_USER_NAME = "_synonyms";
    public static final String SYNONYMS_ROLE_NAME = "_synonyms";

    public static final String REMOTE_MONITORING_NAME = "remote_monitoring_user";
    public static final String REMOTE_MONITORING_COLLECTION_ROLE = "remote_monitoring_collector";
    public static final String REMOTE_MONITORING_INDEXING_ROLE = "remote_monitoring_agent";
    public static final String LAZY_ROLLOVER_NAME = "_lazy_rollover";
    public static final String LAZY_ROLLOVER_ROLE = "_lazy_rollover";
    public static final String REINDEX_DATA_STREAM_NAME = "_reindex_data_stream";
    public static final String REINDEX_DATA_STREAM_ROLE = "_reindex_data_stream";

    private UsernamesField() {}
}
