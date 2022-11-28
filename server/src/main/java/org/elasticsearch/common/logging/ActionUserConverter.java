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
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.action.support.user.ActionUser;
import org.elasticsearch.action.support.user.ActionUserContext;

import java.util.Optional;

@Plugin(category = PatternConverter.CATEGORY, name = "ActionUserConverter")
@ConverterKeys({ "action_user" })
public final class ActionUserConverter extends LogEventPatternConverter {

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ActionUserConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new ActionUserConverter();
    }

    public ActionUserConverter() {
        super("action_user", "action_user");
    }

    private static Optional<ActionUser> getActionUser() {
        return HeaderWarning.THREAD_CONTEXT.stream()
            .map(ActionUserContext::getEffectiveUser)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        getActionUser().ifPresent(user -> toAppendTo.append(user.identity()));
    }

}
