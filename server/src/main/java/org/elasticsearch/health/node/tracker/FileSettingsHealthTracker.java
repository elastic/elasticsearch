/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.node.FileSettingsHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;

/**
 * Houses the current {@link FileSettingsHealthInfo} and provides a means to <i>publish</i> it to the health node.
 */
public class FileSettingsHealthTracker extends HealthTracker<FileSettingsHealthInfo> {
    /**
     * We want a length limit so we don't blow past the indexing limit in the case of a long description string.
     * This is an {@code OperatorDynamic} setting so that if the truncation hampers troubleshooting efforts,
     * the operator could override it and retry the operation without necessarily restarting the cluster.
     */
    public static final String DESCRIPTION_LENGTH_LIMIT_KEY = "fileSettings.descriptionLengthLimit";
    static final Setting<Integer> DESCRIPTION_LENGTH_LIMIT = Setting.intSetting(
        DESCRIPTION_LENGTH_LIMIT_KEY,
        100,
        1, // Need room for the ellipsis
        Setting.Property.OperatorDynamic
    );

    private final Settings settings;
    private FileSettingsHealthInfo currentInfo = FileSettingsHealthInfo.INDETERMINATE;

    public FileSettingsHealthTracker(Settings settings) {
        this.settings = settings;
    }

    public FileSettingsHealthInfo getCurrentInfo() {
        return currentInfo;
    }

    public synchronized void startOccurred() {
        currentInfo = FileSettingsHealthInfo.INITIAL_ACTIVE;
    }

    public synchronized void stopOccurred() {
        currentInfo = currentInfo.inactive();
    }

    public synchronized void changeOccurred() {
        currentInfo = currentInfo.changed();
    }

    public synchronized void successOccurred() {
        currentInfo = currentInfo.successful();
    }

    public synchronized void failureOccurred(String description) {
        currentInfo = currentInfo.failed(limitLength(description));
    }

    private String limitLength(String description) {
        int descriptionLengthLimit = DESCRIPTION_LENGTH_LIMIT.get(settings);
        if (description.length() > descriptionLengthLimit) {
            return description.substring(0, descriptionLengthLimit - 1) + "…";
        } else {
            return description;
        }
    }

    @Override
    protected FileSettingsHealthInfo determineCurrentHealth() {
        return currentInfo;
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, FileSettingsHealthInfo healthInfo) {
        builder.fileSettingsHealthInfo(healthInfo);
    }
}
