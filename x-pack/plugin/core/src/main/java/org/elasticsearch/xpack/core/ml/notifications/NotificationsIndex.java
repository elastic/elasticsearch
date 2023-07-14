/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.template.TemplateUtils;

public final class NotificationsIndex {

    public static final String NOTIFICATIONS_INDEX = ".ml-notifications-000002";

    private static final String RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/";
    private static final String MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";

    private NotificationsIndex() {}

    public static String mapping() {
        return TemplateUtils.loadTemplate(
            RESOURCE_PATH + "notifications_index_mappings.json",
            MlConfigVersion.CURRENT.toString(),
            MAPPINGS_VERSION_VARIABLE
        );
    }
}
