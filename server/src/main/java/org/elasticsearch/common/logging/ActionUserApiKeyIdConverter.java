/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.action.support.user.ActionUser;

@Plugin(category = PatternConverter.CATEGORY, name = "ActionUserApiKeyIdConverter")
@ConverterKeys({ "action_user_apikey_id" })
public final class ActionUserApiKeyIdConverter extends ActionUserConverter {

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ActionUserApiKeyIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new ActionUserApiKeyIdConverter();
    }

    public ActionUserApiKeyIdConverter() {
        super("action_user_apikey_id");
    }

    @Override
    protected void doFormat(ActionUser user, LogEvent event, StringBuilder toAppendTo) {
        final String apiKeyId = (String) user.identifier().asMap().get("apikey_id");
        if (apiKeyId != null) {
            toAppendTo.append(apiKeyId);
        }
    }
}
