/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

/**
 * In tests, use this instead of the EsqlPlugin in order to test ES|QL features
 * using either a:
 *  - an active (non-expired) basic, standard, missing, gold or platinum Elasticsearch license, OR
 *  - an expired enterprise or trial license
 */
public class EsqlPluginWithNonEnterpriseOrExpiredLicense extends EsqlPluginAbstract {
    public EsqlPluginWithNonEnterpriseOrExpiredLicense() {
        super(false);
    }
}
