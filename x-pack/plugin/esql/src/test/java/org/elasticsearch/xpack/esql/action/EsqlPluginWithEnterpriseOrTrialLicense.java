/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

/**
 * In tests, use this instead of the EsqlPlugin in order to use ES|QL features
 * that require an Enteprise (or Trial) license.
 */
public class EsqlPluginWithEnterpriseOrTrialLicense extends EsqlPluginAbstract {
    public EsqlPluginWithEnterpriseOrTrialLicense() {
        super(true);
    }
}
