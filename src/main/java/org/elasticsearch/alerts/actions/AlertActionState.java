/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 */
public enum AlertActionState {

    SEARCH_NEEDED,
    SEARCH_UNDERWAY,
    NO_ACTION_NEEDED,
    ACTION_PERFORMED,
    ERROR;

    @Override
    public String toString(){
        switch (this) {
            case SEARCH_NEEDED:
                return "SEARCH_NEEDED";
            case SEARCH_UNDERWAY:
                return "SEARCH_UNDERWAY";
            case NO_ACTION_NEEDED:
                return "NO_ACTION_NEEDED";
            case ACTION_PERFORMED:
                return "ACTION_PERFORMED";
            case ERROR:
                return "ERROR";
            default:
                return "NO_ACTION_NEEDED";
        }
    }

    public static AlertActionState fromString(String s) {
        switch(s.toUpperCase()) {
            case "SEARCH_NEEDED":
                return SEARCH_NEEDED;
            case "SEARCH_UNDERWAY":
                return SEARCH_UNDERWAY;
            case "NO_ACTION_NEEDED":
                return NO_ACTION_NEEDED;
            case "ACTION_UNDERWAY":
                return ACTION_PERFORMED;
            case "ERROR":
                return ERROR;
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown value [" + s + "] for AlertHistoryState" );
        }
    }
}
