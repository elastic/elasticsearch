/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;

import java.util.Locale;
import java.util.Map;

/**
 * Created by brian on 8/17/14.
 */
public class IndexAlertActionFactory implements AlertActionFactory {
    Client client;


    public IndexAlertActionFactory(Client client){
        this.client = client;
    }

    @Override
    public AlertAction createAction(Object parameters) {
        try {
            if (parameters instanceof Map) {
                Map<String, Object> paramMap = (Map<String, Object>) parameters;
                String index = paramMap.get("index").toString();
                if (!index.toLowerCase(Locale.ROOT).equals(index)) {
                    throw new ElasticsearchIllegalArgumentException("Index names must be all lowercase");
                }

                String type = paramMap.get("type").toString();
                if (!type.toLowerCase(Locale.ROOT).equals(type)) {
                    throw new ElasticsearchIllegalArgumentException("Type names must be all lowercase");
                }

                return new IndexAlertAction(index, type, client);
            } else {
                throw new ElasticsearchIllegalArgumentException("Unable to parse [" + parameters + "] as an IndexAlertAction");
            }
        } catch (Throwable t){
            throw new ElasticsearchIllegalArgumentException("Unable to parse [" + parameters + "] as an IndexAlertAction", t);
        }
    }
}
