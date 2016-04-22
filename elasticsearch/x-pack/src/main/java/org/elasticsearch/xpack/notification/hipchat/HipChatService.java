/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.common.component.LifecycleComponent;

/**
 *
 */
public interface HipChatService extends LifecycleComponent<HipChatService> {

    /**
     * @return The default hipchat account.
     */
    HipChatAccount getDefaultAccount();

    /**
     * @return  The account identified by the given name. If the given name is {@code null} the default
     *          account will be returned.
     */
    HipChatAccount getAccount(String accountName);

}
