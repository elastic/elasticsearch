/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.messages;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

public class MessagesTests extends ESTestCase {

    public void testGetMessage_NoFormatArgs () {
        assertEquals(Messages.DATAFEED_CONFIG_CANNOT_USE_SCRIPT_FIELDS_WITH_AGGS,
                Messages.getMessage(Messages.DATAFEED_CONFIG_CANNOT_USE_SCRIPT_FIELDS_WITH_AGGS));
    }

    public void testGetMessage_WithFormatStrings()  {
        String formattedMessage = Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "field-name", "field-value");
        assertEquals("Invalid field-name value 'field-value' in datafeed configuration", formattedMessage);

        formattedMessage = Messages.getMessage(Messages.JOB_AUDIT_SNAPSHOT_DELETED, "snapshot_foo", "snapshot description");
        assertEquals("Model snapshot [snapshot_foo] with description 'snapshot description' deleted", formattedMessage);
    }
}
