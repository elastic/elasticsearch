/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.pagerduty;

import org.elasticsearch.common.component.LifecycleComponent;

/**
 *
 */
public interface PagerDutyService extends LifecycleComponent<PagerDutyService> {

    PagerDutyAccount getDefaultAccount();

    PagerDutyAccount getAccount(String accountName);
}
