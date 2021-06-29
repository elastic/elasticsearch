/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cloud.gce;

import com.google.api.services.compute.model.Instance;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public interface GceInstancesService extends Closeable {

    /**
     * GCE API Version: Elasticsearch/GceCloud/1.0
     */
    String VERSION = "Elasticsearch/GceCloud/1.0";

    // cloud.gce settings

    /**
     * cloud.gce.project_id: Google project id
     */
    Setting<String> PROJECT_SETTING = Setting.simpleString("cloud.gce.project_id", Property.NodeScope);

    /**
     * cloud.gce.zone: Google Compute Engine zones
     */
    Setting<List<String>> ZONE_SETTING =
        Setting.listSetting("cloud.gce.zone", Collections.emptyList(), Function.identity(), Property.NodeScope);

    /**
     * cloud.gce.refresh_interval: How long the list of hosts is cached to prevent further requests to the AWS API. 0 disables caching.
     * A negative value will cause infinite caching. Defaults to 0s.
     */
    Setting<TimeValue> REFRESH_SETTING =
        Setting.timeSetting("cloud.gce.refresh_interval", TimeValue.timeValueSeconds(0), Property.NodeScope);

    /**
     * cloud.gce.retry: Should we retry calling GCE API in case of error? Defaults to true.
     */
    Setting<Boolean> RETRY_SETTING = Setting.boolSetting("cloud.gce.retry", true, Property.NodeScope);

    /**
     * cloud.gce.max_wait: How long exponential backoff should retry before definitely failing.
     * It's a total time since the initial call is made.
     * A negative value will retry indefinitely. Defaults to `-1s` (retry indefinitely).
     */
    Setting<TimeValue> MAX_WAIT_SETTING =
        Setting.timeSetting("cloud.gce.max_wait", TimeValue.timeValueSeconds(-1), Property.NodeScope);

    /**
     * Return a collection of running instances within the same GCE project
     * @return a collection of running instances within the same GCE project
     */
    Collection<Instance> instances();

    String projectId();

    List<String> zones();
}
