/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Map;

public final class NotificationsIndex {

    public static final String NOTIFICATIONS_INDEX_PREFIX = ".ml-notifications-";
    public static final String NOTIFICATIONS_INDEX_VERSION = "000002";
    public static final String NOTIFICATIONS_INDEX = NOTIFICATIONS_INDEX_PREFIX + NOTIFICATIONS_INDEX_VERSION;
    public static final String NOTIFICATIONS_INDEX_WRITE_ALIAS = ".ml-notifications-write";

    private static final String RESOURCE_PATH = "/ml/";
    private static final String MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";
    public static final int NOTIFICATIONS_INDEX_MAPPINGS_VERSION = 1;
    public static final int NOTIFICATIONS_INDEX_TEMPLATE_VERSION = 1;

    private NotificationsIndex() {}

    public static String mapping() {
        return TemplateUtils.loadTemplate(
            RESOURCE_PATH + "notifications_index_mappings.json",
            MlIndexAndAlias.BWC_MAPPINGS_VERSION, // Only needed for BWC with pre-8.10.0 nodes
            MAPPINGS_VERSION_VARIABLE,
            Map.of("xpack.ml.managed.index.version", Integer.toString(NOTIFICATIONS_INDEX_MAPPINGS_VERSION))
        );
    }
}
