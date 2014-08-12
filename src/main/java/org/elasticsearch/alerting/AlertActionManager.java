/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHitField;

public class AlertActionManager extends AbstractComponent {

    private final AlertManager alertManager;

    @Inject
    public AlertActionManager(Settings settings, AlertManager alertManager) {
        super(settings);
        this.alertManager = alertManager;
    }

    public static AlertAction parseActionFromSearchField(SearchHitField hitField) {
        return null;
    }

    public void doAction(String alertName, AlertResult alertResult){
        Alert alert = alertManager.getAlertForName(alertName);
        alert.action().doAction(alertResult);
    }
}
